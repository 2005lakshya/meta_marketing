# Meta Ads API Service

A small FastAPI service that pulls Meta Ads data from the Graph API and stores it in MongoDB.

## What this project does

- Fetches campaigns, ads, ad sets, and insights from a Meta ad account
- Handles Graph API pagination in a helper function
- Stores data in MongoDB collections with upsert operations
- Exposes endpoints to read stored data and quick stats

## Tech stack

- Python
- FastAPI
- Requests
- MongoDB (PyMongo)
- python-dotenv

## Project structure

- `main.py`: FastAPI app with fetch, store, and read endpoints
- `api.py`, `app.js`, `test.py`: extra files in this workspace

## Prerequisites

- Python 3.9+
- A running MongoDB instance (local or cloud)
- Meta Ads access token and ad account ID

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
```

## Run the API

```bash
uvicorn main:app --reload
```

Server URL:
- `http://127.0.0.1:8000`

For fetching:
- `http://127.0.0.1:8000/docs`


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