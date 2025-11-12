import pandas as pd
from pymongo import MongoClient
import json
import os

# Configuration
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:28017/")
DB_NAME = os.environ.get("DB_NAME", "gamesdb")
COLL_NAME = os.environ.get("COLL_NAME", "games")
CSV_PATH = os.environ.get("CSV_PATH", "vgsales.csv")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "schema.json")

def to_int_maybe(x):
    try:
        xi = int(float(x))
        return xi
    except Exception:
        return None

def main():
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Drop old collection if you want a clean load (comment out for safety)
    if COLL_NAME in db.list_collection_names():
        db.drop_collection(COLL_NAME)

    print("Creating collection with validation...")
    # Create the collection first
    db.create_collection(COLL_NAME)
    # Apply schema validator
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema = json.load(f)
    db.command({
        "collMod": COLL_NAME,
        "validator": schema,
        "validationLevel": "moderate"
    })

    print(f"Reading CSV from {CSV_PATH} ...")
    df = pd.read_csv(CSV_PATH)

    docs = []
    for _, row in df.iterrows():
        year_val = to_int_maybe(row.get("Year"))
        doc = {
            "name": row.get("Name"),
            "platform": row.get("Platform"),
            "year": year_val,
            "genre": row.get("Genre"),
            "publisher": row.get("Publisher"),
            "sales": {
                "na": float(row.get("NA_Sales", 0) or 0),
                "eu": float(row.get("EU_Sales", 0) or 0),
                "jp": float(row.get("JP_Sales", 0) or 0),
                "other": float(row.get("Other_Sales", 0) or 0),
                "global": float(row.get("Global_Sales", 0) or 0),
            },
            "regions": [
                {"region": "NA", "sales": float(row.get("NA_Sales", 0) or 0)},
                {"region": "EU", "sales": float(row.get("EU_Sales", 0) or 0)},
                {"region": "JP", "sales": float(row.get("JP_Sales", 0) or 0)},
                {"region": "Other", "sales": float(row.get("Other_Sales", 0) or 0)},
            ]
        }
        docs.append(doc)

    print(f"Inserting {len(docs)} documents...")
    result = db[COLL_NAME].insert_many(docs, ordered=False)
    print("Inserted count:", len(result.inserted_ids))
    print("Done.")

if __name__ == "__main__":
    main()
