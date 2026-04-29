import os
import json
import requests
from datetime import date, timedelta
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pymongo import MongoClient
from pydantic import BaseModel
from dotenv import load_dotenv
import google.generativeai as genai
import contextlib
import io
import asyncio
import re

load_dotenv()

app = FastAPI()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")
MONGO_URI = os.getenv("MONGO_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL")

if not ACCESS_TOKEN or not AD_ACCOUNT_ID or not MONGO_URI:
    raise Exception("Missing environment variables")

if not GEMINI_API_KEY:
    raise RuntimeError('GEMINI_API_KEY environment variable is not set. Set it before running the application.')

genai.configure(api_key=GEMINI_API_KEY)
_CACHED_GEMINI_MODEL = None
_CACHED_MODEL_LIST = None  # Cache available models list

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
db = client["meta_ads_db"]

campaigns_collection = db["campaigns"]
ads_collection = db["ads"]
adsets_collection = db["adsets"]
insights_collection = db["insights"]
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_FILE = os.path.join(ROOT_DIR, "frontend", "index.html")

WEEKDAY_MAP = {
    "monday": 1,
    "tuesday": 2,
    "wednesday": 3,
    "thursday": 4,
    "friday": 5,
    "saturday": 6,
    "sunday": 7
}

ALLOWED_METRICS = {
    "spend": "spend",
    "clicks": "clicks",
    "impressions": "impressions",
    "reach": "reach",
    "ctr": "ctr",
    "cpc": "cpc",
    "cpm": "cpm",
    "frequency": "frequency",
    "roi": "purchase_roas"
}

ALLOWED_GROUP_BY = {"none", "campaign_name", "adset_name", "ad_name", "date_start"}
ALLOWED_AGG = {"sum", "avg", "max", "min"}


class ChatRequest(BaseModel):
    question: str


def extract_roas(roas_data):
    if isinstance(roas_data, list) and roas_data:
        first = roas_data[0]
        if isinstance(first, dict):
            return to_float(first.get("value"))
    return 0.0


def fetch_all(url, params):
    results = []

    while url:
        res = requests.get(url, params=params)

        if res.status_code != 200:
            print("Error:", res.text)
            break

        data = res.json()
        results.extend(data.get("data", []))

        url = data.get("paging", {}).get("next")
        params = None

    return results


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def get_date_range(date_range: Dict[str, Any]) -> Dict[str, str]:
    today = date.today()
    preset = (date_range or {}).get("preset", "last_90d")

    if preset == "today":
        start = today
        end = today
    elif preset == "yesterday":
        start = today - timedelta(days=1)
        end = start
    elif preset == "last_30d":
        start = today - timedelta(days=29)
        end = today
    elif preset == "last_60d":
        start = today - timedelta(days=59)
        end = today
    elif preset == "last_90d":
        start = today - timedelta(days=89)
        end = today
    elif preset == "custom":
        start = (date_range or {}).get("start")
        end = (date_range or {}).get("end")
        if not start or not end:
            start = (today - timedelta(days=89)).isoformat()
            end = today.isoformat()
        return {"start": start, "end": end}
    else:
        start = today - timedelta(days=89)
        end = today

    return {"start": start.isoformat(), "end": end.isoformat()}


def extract_custom_dates(question: str):
    """Extract custom date range from question text if present."""
    # Pattern: YYYY-MM-DD to YYYY-MM-DD or similar
    pattern = r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})'
    match = re.search(pattern, question)
    if match:
        return {"preset": "custom", "start": match.group(1), "end": match.group(2)}
    return None


def extract_metric_from_question(question: str) -> str:
    """Extract metric from question using keyword matching as fallback."""
    question_lower = question.lower()
    
    # Metric keywords in order of specificity (more specific first to avoid false matches)
    metric_keywords = [
        ("return on ad spend", "roi"),
        ("cost per thousand", "cpm"),
        ("cost per mille", "cpm"),
        ("cost per click", "cpc"),
        ("click-through", "ctr"),
        ("roas", "roi"),
        ("roi", "roi"),
        ("ro", "roi"),  # Handle partial/typo for ROI
        ("cpm", "cpm"),
        ("cpc", "cpc"),
        ("ctr", "ctr"),
        ("click", "clicks"),
        ("impression", "impressions"),
        ("reach", "reach"),
        ("frequency", "frequency"),
        ("spend", "spend"),
        ("cost", "spend"),
        ("expense", "spend"),
    ]
    
    for keyword, metric in metric_keywords:
        if keyword in question_lower:
            return metric
    
    return "spend"  # Default


def extract_group_by_from_question(question: str) -> str:
    """Extract group_by from question using keyword matching as fallback."""
    question_lower = question.lower()
    
    # Priority 1: Explicit breakdown keywords
    if any(kw in question_lower for kw in ["breakdown", "group by", "split by", "per "]):
        if "campaign" in question_lower:
            return "campaign_name"
        if any(kw in question_lower for kw in ["ad set", "adset"]):
            return "adset_name"
        if any(kw in question_lower for kw in ["ad", "ads"]) and "campaign" not in question_lower:
            return "ad_name"
        if any(kw in question_lower for kw in ["date", "day", "daily"]):
            return "date_start"
        return "campaign_name"  # Default breakdown
    
    # Priority 2: "Which [entity]" or "Top [count] [entities]" patterns
    # e.g., "Which campaign had..." → group_by campaign_name
    #       "Top 5 ads by spend" → group_by ad_name
    if any(kw in question_lower for kw in ["which ", "top ", "highest ", "lowest ", "best ", "worst "]):
        # Look for the entity after these keywords
        if "campaign" in question_lower:
            return "campaign_name"
        if any(kw in question_lower for kw in ["ad set", "adset"]):
            return "adset_name"
        if "ad " in question_lower or "ads" in question_lower:
            # Make sure it's not "ad set" or "ad campaign"
            if "ad set" not in question_lower and "ad campaign" not in question_lower:
                return "ad_name"
        if any(kw in question_lower for kw in ["date", "day", "daily"]):
            return "date_start"
    
    # Priority 3: "by [entity]" patterns
    # e.g., "spend by campaign", "roi by ad"
    if " by " in question_lower:
        if "campaign" in question_lower:
            return "campaign_name"
        if any(kw in question_lower for kw in ["ad set", "adset"]):
            return "adset_name"
        if "ad" in question_lower and "ad set" not in question_lower and "ad campaign" not in question_lower:
            return "ad_name"
        if "date" in question_lower:
            return "date_start"
    
    return "none"


def llm_query_plan(question: str) -> Dict[str, Any]:
    """Query Gemini to plan ads analytics query with timeout.
    Falls back to default plan if Gemini is unavailable.
    """
    global _CACHED_GEMINI_MODEL
    
    # First try to extract custom dates from the question itself
    custom_dates = extract_custom_dates(question)
    
    schema_guide = (
        "MongoDB collection insights fields: "
        "campaign_name, adset_name, ad_name, date_start, date_stop, "
        "spend, clicks, impressions, reach, ctr, cpc, cpm, frequency, purchase_roas."
    )

    metric_extraction_guide = (
        "METRIC EXTRACTION RULES:\n"
        "- If question contains: 'spend', 'cost', 'money', 'budget', 'payment', 'expense' → metric: spend\n"
        "- If question contains: 'click', 'ctr' → metric: clicks\n"
        "- If question contains: 'impression', 'view', 'shown' → metric: impressions\n"
        "- If question contains: 'reach', 'people', 'audience' → metric: reach\n"
        "- If question contains: 'cpc', 'cost per click' → metric: cpc\n"
        "- If question contains: 'cpm', 'cost per thousand', 'cost per mille' → metric: cpm\n"
        "- If question contains: 'frequency', 'times shown' → metric: frequency\n"
        "- If question contains: 'roi', 'roas', 'return on ad spend', 'ro' → metric: roi\n"
        "IMPORTANT: Look for the actual metric keyword in the question. Do NOT default to spend if another metric is clearly mentioned."
    )

    group_by_guide = (
        "GROUP BY EXTRACTION RULES:\n"
        "- Question like 'Which campaign had the highest [metric]?' → group_by: campaign_name\n"
        "- Question like 'Which ad had the [metric]?' or 'Which ad set had...' → group_by: ad_name or adset_name\n"
        "- Question like 'Top [N] campaigns by [metric]' → group_by: campaign_name\n"
        "- Question like 'Top [N] ads by [metric]' → group_by: ad_name\n"
        "- Question like '[metric] by campaign' → group_by: campaign_name\n"
        "- Question like '[metric] breakdown by date' → group_by: date_start\n"
        "- If question asks about a specific entity (campaign/ad/adset/date) and wants comparison/ranking → group_by that entity\n"
        "IMPORTANT: If user asks 'Which X had the highest/lowest [metric]?' → ALWAYS group by X. Do NOT use group_by=none."
    )

    system_prompt = (
        "You are a query planner for ads analytics. "
        "Return JSON only with keys: metric, aggregation, group_by, sort, limit, date_range, weekday, filters. "
        "Use only allowed metrics: spend, clicks, impressions, reach, ctr, cpc, cpm, frequency, roi. "
        "Use aggregation: sum, avg, max, min. "
        "Use group_by: none, campaign_name, adset_name, ad_name, date_start. "
        "Use sort: asc or desc. "
        "For date_range: if question mentions specific dates like '2026-04-01 to 2026-04-30' or 'from X to Y', use preset 'custom' with exact start and end dates in YYYY-MM-DD format. "
        "Otherwise use preset from: today, yesterday, last_7d, last_30d, last_60d. "
        "weekday must be one of monday..sunday or none. "
        "filters must be list of objects: {field, op, value}. op must be eq or contains. "
        f"{schema_guide}\n\n{metric_extraction_guide}\n\n{group_by_guide}"
    )

    prompt = f"{system_prompt}\n\nUser Question: {question}"

    # Try models in order without discovery
    models_to_try = []
    if GEMINI_MODEL:
        models_to_try.append(GEMINI_MODEL)
    if _CACHED_GEMINI_MODEL:
        models_to_try.append(_CACHED_GEMINI_MODEL)
    # Add commonly available models in order of preference
    models_to_try.extend(["gemini-2.0-flash", "gemini-1.5-flash", "gemini-2.0-flash-exp", "gemini-pro", "gemini-1.5-pro-latest"])
    
    for model_name in models_to_try:
        try:
            print(f"Trying model: {model_name}")
            with open(os.devnull, 'w') as devnull:
                with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                    m = genai.GenerativeModel(model_name)
                    resp = m.generate_content(prompt)
            text = getattr(resp, 'text', None) or getattr(resp, 'content', None) or str(resp)
            parsed = json.loads(text)
            _CACHED_GEMINI_MODEL = model_name
            print(f"Success with model: {model_name}")
            
            # If LLM didn't extract custom dates but they exist in question, override
            if custom_dates and (parsed.get("date_range") or {}).get("preset") != "custom":
                parsed["date_range"] = custom_dates
                print("Extracted custom dates from question")
            
            return parsed
        except json.JSONDecodeError:
            print(f"Model {model_name} returned invalid JSON")
            continue
        except Exception as e:
            print(f"Model {model_name} failed: {type(e).__name__}: {str(e)[:100]}")
            continue
    
    # Default fallback plan
    print("All models failed, using default plan")
    plan = {
        "metric": "spend",
        "aggregation": "sum",
        "group_by": "none",
        "sort": "desc",
        "limit": 10,
        "weekday": "none",
        "filters": []
    }
    
    # Use extracted custom dates if available
    if custom_dates:
        plan["date_range"] = custom_dates
    else:
        plan["date_range"] = {"preset": "last_90d"}
    
    return plan


def validate_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    # Helper to extract single value from list or string
    def _get_string(value):
        if isinstance(value, list) and value:
            return str(value[0])
        return str(value) if value else ""
    
    metric = (_get_string(plan.get("metric") or "spend")).lower()
    aggregation = (_get_string(plan.get("aggregation") or "sum")).lower()
    group_by = (_get_string(plan.get("group_by") or "none")).lower()
    sort = (_get_string(plan.get("sort") or "desc")).lower()
    limit = to_int(plan.get("limit") or 5)
    weekday = (_get_string(plan.get("weekday") or "none")).lower()
    filters = plan.get("filters") or []

    if metric not in ALLOWED_METRICS:
        metric = "spend"
    if aggregation not in ALLOWED_AGG:
        aggregation = "sum"
    if group_by not in ALLOWED_GROUP_BY:
        group_by = "none"
    if sort not in {"asc", "desc"}:
        sort = "desc"
    if weekday not in WEEKDAY_MAP and weekday != "none":
        weekday = "none"

    safe_filters = []
    for item in filters:
        if not isinstance(item, dict):
            continue
        field = item.get("field")
        op = item.get("op")
        value = item.get("value")
        if field in {"campaign_name", "adset_name", "ad_name"} and op in {"eq", "contains"}:
            safe_filters.append({"field": field, "op": op, "value": str(value)})

    return {
        "metric": metric,
        "aggregation": aggregation,
        "group_by": group_by,
        "sort": sort,
        "limit": max(1, min(limit, 25)),
        "weekday": weekday,
        "date_range": get_date_range(plan.get("date_range") or {"preset": "last_7d"}),
        "filters": safe_filters
    }


def build_pipeline(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    metric_field = ALLOWED_METRICS[plan["metric"]]
    agg_map = {
        "sum": "$sum",
        "avg": "$avg",
        "max": "$max",
        "min": "$min"
    }

    pipeline: List[Dict[str, Any]] = []

    # Build match filter for explicit filters only (date is already filtered by API)
    match: Dict[str, Any] = {}

    for f in plan["filters"]:
        if f["op"] == "eq":
            match[f["field"]] = f["value"]
        elif f["op"] == "contains":
            match[f["field"]] = {"$regex": f["value"], "$options": "i"}

    if match:
        pipeline.append({"$match": match})

    # If grouping by a field that might be null, try to fill it from related collections
    if plan["group_by"] == "campaign_name":
        # Lookup ads to get campaign_id if missing
        pipeline.append({
            "$lookup": {
                "from": "ads",
                "localField": "ad_id",
                "foreignField": "ad_id",
                "as": "ad_info"
            }
        })
        # If campaign_name is null but we found an ad, use its campaign_id to lookup campaign name
        pipeline.append({
            "$lookup": {
                "from": "campaigns",
                "let": {
                    "cid": {
                        "$cond": [
                            {"$and": [{"$eq": ["$campaign_id", None]}, {"$gt": [{"$size": "$ad_info"}, 0]}]},
                            {"$arrayElemAt": ["$ad_info.campaign_id", 0]},
                            "$campaign_id"
                        ]
                    }
                },
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$campaign_id", "$$cid"]}}}
                ],
                "as": "campaign_info"
            }
        })
        pipeline.append({
            "$addFields": {
                "campaign_name": {
                    "$cond": [
                        {"$and": [{"$eq": ["$campaign_name", None]}, {"$gt": [{"$size": "$campaign_info"}, 0]}]},
                        {"$arrayElemAt": ["$campaign_info.name", 0]},
                        {"$cond": [{"$eq": ["$campaign_name", None]}, "Unknown", "$campaign_name"]}
                    ]
                }
            }
        })
        pipeline.append({"$project": {"ad_info": 0, "campaign_info": 0}})

    if plan["weekday"] != "none":
        pipeline.append({
            "$addFields": {
                "weekday_num": {"$isoDayOfWeek": {"$toDate": "$date_start"}}
            }
        })
        pipeline.append({"$match": {"weekday_num": WEEKDAY_MAP[plan["weekday"]]}})

    group_id: Any = None if plan["group_by"] == "none" else f"${plan['group_by']}"
    pipeline.append({
        "$group": {
            "_id": group_id,
            "value": {agg_map[plan["aggregation"]]: f"${metric_field}"}
        }
    })

    sort_order = -1 if plan["sort"] == "desc" else 1
    pipeline.append({"$sort": {"value": sort_order}})

    if plan["group_by"] != "none":
        pipeline.append({"$limit": plan["limit"]})

    pipeline.append({
        "$project": {
            "_id": 0,
            "label": {"$ifNull": ["$_id", "overall"]},
            "value": {"$round": ["$value", 4]}
        }
    })
    return pipeline


def format_number(value: float, metric: str) -> str:
    """Format a number with appropriate currency or unit."""
    if metric in ["spend", "cpc", "cpm"]:
        return f"${value:,.2f}"
    elif metric in ["ctr"]:
        return f"{value:.2f}%"
    else:
        # For clicks, impressions, reach - use whole numbers
        if value == int(value):
            return f"{int(value):,}"
        else:
            return f"{value:,.2f}"


def summarize_answer(question: str, plan: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return (
            "I don't have data for that query in the selected time range. "
            "Try asking about a longer period or checking available ads and campaigns."
        )

    metric = plan["metric"].lower()
    metric_display = metric.upper() if metric != "roi" else "ROI"
    start = plan["date_range"]["start"]
    end = plan["date_range"]["end"]

    # Friendly metric names
    metric_names = {
        "spend": "spending",
        "clicks": "clicks", 
        "impressions": "impressions",
        "reach": "reach",
        "ctr": "click-through rate",
        "cpc": "cost per click",
        "cpm": "cost per thousand impressions",
        "frequency": "frequency",
        "roi": "return on ad spend"
    }
    friendly_metric = metric_names.get(metric, metric)

    # Debug: print what we got
    print(f"DEBUG: rows={rows}")

    if plan["group_by"] == "none":
        # Single aggregated result
        if not rows or 'value' not in rows[0]:
            return f"No data found for {friendly_metric}."
        
        value = rows[0].get('value', 0)
        formatted_value = format_number(value, metric)
        
        # Natural language responses
        responses = {
            "spend": f"Your total {friendly_metric} from {start} to {end} was {formatted_value}.",
            "clicks": f"You got {formatted_value} clicks from {start} to {end}.",
            "impressions": f"Your ads reached {formatted_value} impressions from {start} to {end}.",
            "reach": f"Your ads reached {formatted_value} people from {start} to {end}.",
            "cpc": f"Your average cost per click was {formatted_value} from {start} to {end}.",
            "cpm": f"Your average cost per thousand impressions was {formatted_value} from {start} to {end}.",
            "ctr": f"Your click-through rate was {formatted_value} from {start} to {end}.",
            "frequency": f"On average, your ads were shown {formatted_value} times per person from {start} to {end}.",
            "roi": f"Your return on ad spend was {formatted_value} from {start} to {end}."
        }
        return responses.get(metric, f"Your {friendly_metric} was {formatted_value} from {start} to {end}.")

    # For grouped results
    if not rows or 'value' not in rows[0]:
        return f"No breakdown available for {friendly_metric} by {plan['group_by']}."
    
    top = rows[0]
    top_value = format_number(top.get('value', 0), metric)
    
    group_by = plan["group_by"]
    group_display = {
        "campaign_name": "campaign",
        "adset_name": "ad set",
        "ad_name": "ad",
        "date_start": "date"
    }.get(group_by, group_by)
    
    top_label = top.get('label', 'Unknown')
    response = f"Here's the breakdown by {group_display} from {start} to {end}:\n"
    response += f"• Top performer: **{top_label}** with {top_value} {friendly_metric}"
    
    if len(rows) > 1:
        response += f"\n• Total results shown: {len(rows)} {group_display}(s)"
    
    return response


@app.get("/")
def home():
    if os.path.exists(FRONTEND_FILE):
        return FileResponse(FRONTEND_FILE)
    return {"message": "Frontend not found. Open /docs for API usage."}


@app.post("/test-api-key")
def test_api_key():
    """Test if Gemini API key is working."""
    try:
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                model = genai.GenerativeModel("gemini-2.5-flash")
                response = model.generate_content("Say 'API key is working' in one sentence")
        
        if response and hasattr(response, 'text'):
            return {
                "status": "success",
                "message": "API key is working",
                "model_used": "gemini-2.5-flash",
                "response": response.text[:100]
            }
        else:
            return {
                "status": "error",
                "message": "API key responded but with unexpected format"
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"API key test failed: {type(e).__name__}",
            "detail": str(e)
        }


@app.post("/chat")
async def chat_with_ads(request: ChatRequest):
    question = (request.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    # Detect if asking for list of ads/campaigns/adsets vs analytics
    question_lower = question.lower()
    
    # Check for analytical keywords (metrics, comparisons) - these indicate analytics query, not list query
    analytical_keywords = ['roi', 'roas', 'spend', 'clicks', 'impressions', 'reach', 'cpc', 'cpm', 'ctr', 'frequency', 'cost',
                          'highest', 'lowest', 'top', 'best', 'worst', 'most', 'least', 'average', 'total', 'sum', 'max', 'min',
                          'breakdown', 'by spend', 'by roi', 'by clicks', 'performance', 'metrics']
    has_analytics = any(kw in question_lower for kw in analytical_keywords)
    
    is_list_query = any(keyword in question_lower for keyword in [
        'list', 'show', 'get', 'which', 'active', 'all', 'tell me', 'what are', 'give', 'detailed'
    ])
    
    # If question has analytics keywords, it's NOT a list query - it's an analytics query
    if has_analytics:
        is_list_query = False
    
    if is_list_query and any(word in question_lower for word in ['ad', 'campaign', 'adset', 'insight']):
        # Handle data listing queries - query all collections
        try:
            rows = []
            answer = ""
            is_detailed = 'detailed' in question_lower or 'details' in question_lower
            
            if 'adset' in question_lower:
                rows = list(adsets_collection.find({}, {"_id": 0}).limit(25))
                if is_detailed:
                    answer = f"Here are detailed records for {len(rows)} ad sets:\n"
                    for r in rows:
                        answer += f"• **{r.get('name', 'Unnamed')}** (ID: {r.get('adset_id')}, Status: {r.get('status')}, Budget: ${r.get('daily_budget', 0)})\n"
                else:
                    answer = (
                        f"Here are all {len(rows)} ad sets in your account:\n" +
                        "\n".join([f"• **{r.get('name', 'Unnamed')}**" for r in rows])
                    )
            elif 'campaign' in question_lower:
                rows = list(campaigns_collection.find({}, {"_id": 0}).limit(25))
                if is_detailed:
                    answer = f"Here are detailed records for {len(rows)} campaigns:\n"
                    for r in rows:
                        answer += f"• **{r.get('name', 'Unnamed')}** (ID: {r.get('campaign_id')}, Status: {r.get('status')}, Objective: {r.get('objective')})\n"
                else:
                    answer = (
                        f"Here are all {len(rows)} campaigns in your account:\n" +
                        "\n".join([f"• **{r.get('name', 'Unnamed')}**" for r in rows])
                    )
            elif 'insight' in question_lower:
                rows = list(insights_collection.find({}, {"_id": 0}).limit(25))
                answer = (
                    f"Here are all {len(rows)} insights:\n" +
                    "\n".join([f"• {r.get('date_start')}: Spend: ${r.get('spend', 0):.2f}, Clicks: {r.get('clicks', 0)}, Impressions: {r.get('impressions', 0)}" for r in rows])
                )
            else:  # ads
                rows = list(ads_collection.find({}, {"_id": 0}).limit(25))
                if is_detailed:
                    answer = f"Here are detailed records for {len(rows)} ads:\n"
                    for r in rows:
                        answer += f"• **{r.get('name', 'Unnamed')}** (ID: {r.get('ad_id')}, Status: {r.get('status')}, AdSet ID: {r.get('adset_id')})\n"
                else:
                    answer = (
                        f"Here are all {len(rows)} ads in your account:\n" +
                        "\n".join([f"• **{r.get('name', 'Unnamed')}**" for r in rows])
                    )
            
            return {
                "answer": answer,
                "plan": {"type": "list_query", "detailed": is_detailed},
                "rows": rows
            }
        except Exception as e:
            print(f"List query error: {e}")
            return {
                "answer": "Could not retrieve list.",
                "plan": {"type": "list_query"},
                "rows": []
            }
    
    # Handle analytics queries - use insights collection
    try:
        # Run LLM query in thread pool to not block event loop
        raw_plan = await asyncio.to_thread(llm_query_plan, question)
    except Exception as e:
        # Fallback plan if Gemini times out
        print(f"Gemini API error: {e}")
        raw_plan = {
            "metric": "spend",
            "aggregation": "sum",
            "group_by": "none",
            "sort": "desc",
            "limit": 10,
            "date_range": {"preset": "last_90d"},
            "weekday": "none",
            "filters": []
        }
    
    safe_plan = validate_plan(raw_plan)
    
    # Fallback: Use keyword-based extraction if LLM might have missed it
    detected_metric = extract_metric_from_question(question)
    if detected_metric and detected_metric != "spend":
        # Override if the detected metric is not the default
        safe_plan["metric"] = detected_metric
        print(f"Fallback metric detection: {detected_metric}")

    detected_group_by = extract_group_by_from_question(question)
    if detected_group_by and detected_group_by != "none" and safe_plan["group_by"] == "none":
        # Override if user explicitly asked for breakdown but LLM missed it
        safe_plan["group_by"] = detected_group_by
        print(f"Fallback group_by detection: {detected_group_by}")

    # Query MongoDB insights collection with the validated plan
    rows = []
    try:
        pipeline = build_pipeline(safe_plan)
        rows = list(insights_collection.aggregate(pipeline))
    except Exception as e:
        print(f"MongoDB query error: {e}")
    
    # If no insights data, try to get summary from all collections
    if not rows:
        try:
            total_campaigns = campaigns_collection.count_documents({})
            total_ads = ads_collection.count_documents({})
            total_adsets = adsets_collection.count_documents({})
            total_insights = insights_collection.count_documents({})
            
            rows = [{
                "label": "database_summary",
                "campaigns": total_campaigns,
                "ads": total_ads,
                "adsets": total_adsets,
                "insights": total_insights
            }]
        except:
            pass
    
    answer = summarize_answer(question, safe_plan, rows)

    return {
        "answer": answer,
        "plan": safe_plan,
        "rows": rows
    }



@app.get("/fetch-campaigns")
def fetch_campaigns():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/campaigns"

    params = {
        "fields": "id,name,status,effective_status,configured_status,objective,buying_type,start_time,stop_time,daily_budget,lifetime_budget,bid_strategy,special_ad_categories,created_time,updated_time",
        "access_token": ACCESS_TOKEN
    }

    campaigns = fetch_all(url, params)

    for c in campaigns:
        campaign_doc = {"campaign_id": c["id"], **c}
        campaigns_collection.update_one(
            {"campaign_id": c["id"]},
            {"$set": campaign_doc},
            upsert=True
        )

    return {"message": "Campaigns stored", "count": len(campaigns)}




@app.get("/fetch-ads")
def fetch_ads():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/ads"

    params = {
        "fields": "id,name,status,effective_status,configured_status,adset_id,campaign_id,creative,bid_amount,conversion_domain,tracking_specs,created_time,updated_time",
        "access_token": ACCESS_TOKEN
    }

    ads = fetch_all(url, params)

    for ad in ads:
        ad_doc = {"ad_id": ad["id"], **ad}
        ads_collection.update_one(
            {"ad_id": ad["id"]},
            {"$set": ad_doc},
            upsert=True
        )

    return {"message": "Ads stored", "count": len(ads)}




@app.get("/fetch-adsets")
def fetch_adsets():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/adsets"

    params = {
        "fields": "id,name,campaign_id,status,effective_status,daily_budget,lifetime_budget,bid_amount,billing_event,optimization_goal,targeting,start_time,end_time,created_time,updated_time",
        "access_token": ACCESS_TOKEN
    }

    adsets = fetch_all(url, params)

    for a in adsets:
        adset_doc = {"adset_id": a["id"], **a}
        adsets_collection.update_one(
            {"adset_id": a["id"]},
            {"$set": adset_doc},
            upsert=True
        )

    return {"message": "AdSets stored", "count": len(adsets)}



@app.get("/fetch-insights")
def fetch_insights():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/insights"

    params = {
        "fields": "account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,date_start,date_stop,impressions,reach,frequency,clicks,unique_clicks,inline_link_clicks,spend,ctr,cpc,cpm,cpp,purchase_roas,actions,cost_per_action_type,conversions",
        "level": "ad",
        "date_preset": "last_90d",
        "access_token": ACCESS_TOKEN
    }

    insights = fetch_all(url, params)

    for i in insights:
        insights_doc = {
            "ad_id": i.get("ad_id"),
            "account_id": i.get("account_id"),
            "campaign_id": i.get("campaign_id"),
            "campaign_name": i.get("campaign_name"),
            "adset_id": i.get("adset_id"),
            "adset_name": i.get("adset_name"),
            "ad_name": i.get("ad_name"),
            "date_start": i.get("date_start"),
            "date_stop": i.get("date_stop"),
            "impressions": to_int(i.get("impressions")),
            "reach": to_int(i.get("reach")),
            "frequency": to_float(i.get("frequency")),
            "clicks": to_int(i.get("clicks")),
            "unique_clicks": to_int(i.get("unique_clicks")),
            "inline_link_clicks": to_int(i.get("inline_link_clicks")),
            "spend": to_float(i.get("spend")),
            "ctr": to_float(i.get("ctr")),
            "cpc": to_float(i.get("cpc")),
            "cpm": to_float(i.get("cpm")),
            "cpp": to_float(i.get("cpp")),
            "purchase_roas": extract_roas(i.get("purchase_roas")),
            "actions": i.get("actions") or [],
            "cost_per_action_type": i.get("cost_per_action_type") or [],
            "conversions": i.get("conversions") or []
        }
        insights_collection.update_one(
            {
                "ad_id": i.get("ad_id"),
                "date_start": i.get("date_start"),
                "date_stop": i.get("date_stop")
            },
            {"$set": insights_doc},
            upsert=True
        )

    return {"message": "Insights stored", "count": len(insights)}


@app.get("/campaigns")
def get_campaigns():
    return list(campaigns_collection.find({}, {"_id": 0}))

@app.get("/ads")
def get_ads():
    return list(ads_collection.find({}, {"_id": 0}))

@app.get("/adsets")
def get_adsets():
    return list(adsets_collection.find({}, {"_id": 0}))

@app.get("/insights")
def get_insights():
    return list(insights_collection.find({}, {"_id": 0}))



@app.get("/stats")
def stats():
    return {
        "campaigns": campaigns_collection.count_documents({}),
        "ads": ads_collection.count_documents({}),
        "adsets": adsets_collection.count_documents({}),
        "insights": insights_collection.count_documents({})
    }