import os
import requests
from fastapi import FastAPI
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")
MONGO_URI = os.getenv("MONGO_URI")

if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
    raise Exception("Missing environment variables")

client = MongoClient(MONGO_URI)
db = client["meta_ads_db"]

campaigns_collection = db["campaigns"]
ads_collection = db["ads"]
adsets_collection = db["adsets"]
insights_collection = db["insights"]


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



@app.get("/fetch-campaigns")
def fetch_campaigns():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/campaigns"

    params = {
        "fields": "id,name,status,objective",
        "access_token": ACCESS_TOKEN
    }

    campaigns = fetch_all(url, params)

    for c in campaigns:
        campaigns_collection.update_one(
            {"campaign_id": c["id"]},
            {"$set": {
                "name": c.get("name"),
                "status": c.get("status"),
                "objective": c.get("objective")
            }},
            upsert=True
        )

    return {"message": "Campaigns stored", "count": len(campaigns)}




@app.get("/fetch-ads")
def fetch_ads():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/ads"

    params = {
        "fields": "id,name,status,adset_id,campaign_id",
        "access_token": ACCESS_TOKEN
    }

    ads = fetch_all(url, params)

    for ad in ads:
        ads_collection.update_one(
            {"ad_id": ad["id"]},
            {"$set": {
                "name": ad.get("name"),
                "status": ad.get("status"),
                "adset_id": ad.get("adset_id"),
                "campaign_id": ad.get("campaign_id")
            }},
            upsert=True
        )

    return {"message": "Ads stored", "count": len(ads)}




@app.get("/fetch-adsets")
def fetch_adsets():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/adsets"

    params = {
        "fields": "id,name,campaign_id,daily_budget,status",
        "access_token": ACCESS_TOKEN
    }

    adsets = fetch_all(url, params)

    for a in adsets:
        adsets_collection.update_one(
            {"adset_id": a["id"]},
            {"$set": {
                "name": a.get("name"),
                "campaign_id": a.get("campaign_id"),
                "daily_budget": a.get("daily_budget"),
                "status": a.get("status")
            }},
            upsert=True
        )

    return {"message": "AdSets stored", "count": len(adsets)}



@app.get("/fetch-insights")
def fetch_insights():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/insights"

    params = {
        "fields": "ad_id,impressions,clicks,spend,ctr,cpc",
        "level": "ad",
        "date_preset": "last_7d",
        "access_token": ACCESS_TOKEN
    }

    insights = fetch_all(url, params)

    for i in insights:
        insights_collection.update_one(
            {"ad_id": i.get("ad_id")},
            {"$set": {
                "impressions": int(i.get("impressions") or 0),
                "clicks": int(i.get("clicks") or 0),
                "spend": float(i.get("spend") or 0),
                "ctr": float(i.get("ctr") or 0),
                "cpc": float(i.get("cpc") or 0)
            }},
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