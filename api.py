import requests
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")
MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)
db = client["meta_ads_db"]

ads_collection = db["ads"]
campaigns_collection = db["campaigns"]
adsets_collection = db["adsets"]
insights_collection = db["insights"]


def fetch_ads():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/ads"
    params = {
        "fields": "id,name,status,adset_id,campaign_id",
        "access_token": ACCESS_TOKEN
    }

    data = requests.get(url, params=params).json()

    for ad in data.get("data", []):
        ads_collection.insert_one({
            "ad_id": ad["id"],
            "name": ad.get("name"),
            "status": ad.get("status"),
            "adset_id": ad.get("adset_id"),
            "campaign_id": ad.get("campaign_id")
        })


def fetch_campaigns():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/campaigns"
    params = {
        "fields": "id,name,status,objective",
        "access_token": ACCESS_TOKEN
    }

    data = requests.get(url, params=params).json()

    for c in data.get("data", []):
        campaigns_collection.insert_one({
            "campaign_id": c["id"],
            "name": c["name"],
            "status": c.get("status"),
            "objective": c.get("objective")
        })


def fetch_adsets():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/adsets"
    params = {
        "fields": "id,name,campaign_id,daily_budget,status",
        "access_token": ACCESS_TOKEN
    }

    data = requests.get(url, params=params).json()

    for a in data.get("data", []):
        adsets_collection.insert_one({
            "adset_id": a["id"],
            "campaign_id": a.get("campaign_id"),
            "name": a.get("name"),
            "daily_budget": a.get("daily_budget"),
            "status": a.get("status")
        })


def fetch_insights():
    url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "fields": "ad_id,impressions,clicks,spend,ctr,cpc",
        "access_token": ACCESS_TOKEN
    }

    data = requests.get(url, params=params).json()

    for i in data.get("data", []):
        insights_collection.insert_one({
            "ad_id": i.get("ad_id"),
            "impressions": int(i.get("impressions", 0)),
            "clicks": int(i.get("clicks", 0)),
            "spend": float(i.get("spend", 0)),
            "ctr": float(i.get("ctr", 0)),
            "cpc": float(i.get("cpc", 0))
        })


def main():
    fetch_campaigns()
    fetch_adsets()
    fetch_ads()
    fetch_insights()
    print("Data fetched and stored successfully!")

if __name__ == "__main__":
    main()