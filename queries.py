from pymongo import MongoClient
import os
import pprint

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:28017/")
DB_NAME = os.environ.get("DB_NAME", "gamesdb")
COLL_NAME = os.environ.get("COLL_NAME", "games")

pp = pprint.PrettyPrinter(indent=2)

def connect():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]

def select_all():
    coll = connect()
    print("\n--- All documents (first N) ---")
    try:
        n = int(input("How many docs would you like to print? (e.g., 5): ").strip() or "5")
    except:
        n = 5
    for doc in coll.find().limit(n):
        pp.pprint(doc)

def select_embedded():
    coll = connect()
    region = input("Region (NA/EU/JP/Other): ").strip() or "EU"
    try:
        min_sales = float(input("Minimum sales in that region (e.g., 20): ").strip() or "20")
    except:
        min_sales = 20.0

    query = {
        "regions": {
            "$elemMatch": {
                "region": region,
                "sales": { "$gt": min_sales }
            }
        }
    }
    print("\n--- Embedded array query ---")
    for doc in coll.find(query, {"name":1, "regions":1, "_id":0}).limit(20):
        pp.pprint(doc)

def select_with_projection():
    coll = connect()
    projection = {"_id":0, "name":1, "platform":1, "sales.global":1}
    print("\n--- Projection ---")
    for doc in coll.find({}, projection).limit(10):
        pp.pprint(doc)

def select_sorted():
    coll = connect()
    field = input("Sort by which field? (e.g., sales.global): ").strip() or "sales.global"
    order = input("Order asc/desc (asc=1, desc=-1): ").strip() or "-1"
    try:
        order_int = int(order)
        if order_int not in (1, -1):
            order_int = -1
    except:
        order_int = -1

    print("\n--- Sorted selection ---")
    for doc in coll.find({}, {"name":1, "sales.global":1, "_id":0}).sort(field, order_int).limit(10):
        pp.pprint(doc)

def aggregation_example():
    coll = connect()
    try:
        year_min = int(input("Aggregate from year >= (e.g., 2000): ").strip() or "2000")
    except:
        year_min = 2000

    pipeline = [
        {"$match": {"year": {"$gte": year_min}}},
        {"$group": {"_id": "$genre", "total_sales": {"$sum": "$sales.global"}}},
        {"$sort": {"total_sales": -1}},
    ]
    print("\n--- Aggregation (top genres by total sales) ---")
    for doc in coll.aggregate(pipeline):
        pp.pprint(doc)

MENU = """
Choose an option:
  1) Select ALL documents (JSON)
  2) Select embedded array data with criteria
  3) Select with PROJECTION
  4) Select with SORT
  5) Aggregation pipeline ($match, $group, $sort)
  q) Quit
"""

def main():
    coll = connect()
    coll.database.command("ping")
    while True:
        print(MENU)
        choice = input("Enter choice: ").strip().lower()
        if choice == "1":
            select_all()
        elif choice == "2":
            select_embedded()
        elif choice == "3":
            select_with_projection()
        elif choice == "4":
            select_sorted()
        elif choice == "5":
            aggregation_example()
        elif choice in ("q", "quit", "exit"):
            print("Bye.")
            break

if __name__ == "__main__":
    main()
