# Meta Ads API Service

A small FastAPI service that pulls Meta Ads data from the Graph API and stores it in MongoDB.
It also includes an LLM-powered chatbot interface to query ads data using natural language.

## What this project does

- Fetches campaigns, ads, ad sets, and insights from a Meta ad account
- Handles Graph API pagination in a helper function
- Stores data in MongoDB collections with upsert operations
- Exposes endpoints to read stored data and quick stats
- Provides a web chatbot that turns natural language questions into safe MongoDB aggregations

## Tech stack

- Python
- FastAPI
- Requests
- MongoDB (PyMongo)
- python-dotenv
- OpenAI Chat Completions API (optional but recommended for better query interpretation)

## Project structure

- `main.py`: FastAPI app with fetch, store, read endpoints, and chatbot endpoint
- `frontend/index.html`: browser UI for the chatbot
- `api.py`, `app.js`, `test.py`: extra files in this workspace

## Prerequisites

- Python 3.9+
- A running MongoDB instance (local or cloud)
- Meta Ads access token and ad account ID
- OpenAI API key (optional fallback exists, but key is recommended)

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install fastapi uvicorn requests pymongo python-dotenv
```

3. Create a `.env` file in the project root:

```env
ACCESS_TOKEN=your_meta_access_token
AD_ACCOUNT_ID=act_1234567890
MONGO_URI=mongodb://localhost:27017
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4o-mini
```

## Run the API

```bash
uvicorn main:app --reload
```

Server URL:
- `http://127.0.0.1:8000`

For API docs:
- `http://127.0.0.1:8000/docs`

For chatbot UI:
- `http://127.0.0.1:8000/`


## Endpoints
Fetch and store from Meta:
- `GET /fetch-campaigns`
- `GET /fetch-ads`
- `GET /fetch-adsets`
- `GET /fetch-insights`

Read stored data:
- `GET /campaigns`
- `GET /ads`
- `GET /adsets`
- `GET /insights`

Stats:
- `GET /stats`

Chatbot:
- `POST /chat`
	- Request body:

```json
{
	"question": "Top 5 campaigns by spend in last 30 days"
}
```

	- Response shape:

```json
{
	"answer": "From 2026-03-25 to 2026-04-23, top result by SPEND is Campaign A with value 291.44. Returned 5 grouped rows.",
	"plan": {
		"metric": "spend",
		"aggregation": "sum",
		"group_by": "campaign_name",
		"sort": "desc",
		"limit": 5,
		"weekday": "none",
		"date_range": {
			"start": "2026-03-25",
			"end": "2026-04-23"
		},
		"filters": []
	},
	"rows": [
		{
			"label": "Campaign A",
			"value": 291.44
		}
	]
}
```

## Chatbot Design

The chatbot uses a Text-to-Query strategy for MongoDB:

1. User asks a natural language question.
2. LLM returns a strict JSON query plan.
3. Backend validates the plan with allowlists.
4. Backend builds Mongo aggregation pipeline and executes it.
5. API returns a human-readable summary and structured rows.

### Prompt Engineering

System prompt goals used in `main.py`:

- Force JSON-only output (no prose)
- Restrict metrics to known fields (`spend`, `clicks`, `impressions`, `ctr`, `cpc`, `cpm`, `reach`, `frequency`, `roi`)
- Restrict groupings to known dimensions (`campaign_name`, `adset_name`, `ad_name`, `date_start`, `none`)
- Restrict date presets (`today`, `yesterday`, `last_7d`, `last_30d`, `custom`)
- Restrict filters and operators (`eq`, `contains`)

This reduces invalid query generation and keeps query behavior deterministic.

### Error Handling

- Empty question returns HTTP 400.
- Invalid or unsupported plan values are normalized to safe defaults.
- If no rows match, API returns a friendly message with retry guidance.
- If ROI data is missing, API explicitly reports missing data and next step.
- If LLM fails, backend falls back to a default safe plan (spend over last 7 days).

### Security Notes (SQL/Query Injection Risk)

Even with MongoDB, prompt-to-query systems have injection-like risk if model output is trusted directly.

Mitigations used:

- Never execute raw model text as query logic.
- Validate every plan key against strict allowlists.
- Restrict operators to a small safe subset.
- Cap result limits and keep queries aggregation-only.

Additional production mitigations recommended:

- Add authentication and per-user rate limiting.
- Add query cost limits and timeout controls.
- Add audit logging for prompts, plans, and executed pipelines.
- Consider a read-only analytics database user for chatbot queries.

### Scaling Strategy (Millions of Rows)

When dataset size grows large:

1. Keep LLM context schema-only (never send raw table/collection data).
2. Push heavy computation to MongoDB aggregations and indexes.
3. Add indexes on `date_start`, `campaign_name`, `adset_name`, `ad_name`, and high-traffic filter fields.
4. Build pre-aggregated daily summary collections for common dashboards and bot queries.
5. Add caching for repeated questions (for example: last 7 day spend).
6. Return only compact result sets to the LLM (or skip second LLM pass and format in backend).