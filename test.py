from pymongo import MongoClient

client = MongoClient(MONGO_URI)

db = client["meta_ads_db"]

collection = db["test"]

collection.insert_one({"name": "Lakshya", "project": "Meta Ads"})

print(list(collection.find()))