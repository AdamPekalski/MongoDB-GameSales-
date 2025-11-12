from pymongo import MongoClient, ReturnDocument
import os
import pprint

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:28017/")
DB_NAME = os.environ.get("DB_NAME", "gamesdb")
COLL_NAME = os.environ.get("COLL_NAME", "games")

pp = pprint.PrettyPrinter(indent=2)

def connect():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]

def insert_game():
    coll = connect()
    print("Enter new game fields. (Leave blank to use defaults)")
    name = input("name: ").strip() or "Example Game"
    platform = input("platform: ").strip() or "PC"
    try:
        year = int(input("year (e.g., 2024): ").strip() or "2024")
    except:
        year = 2024
    genre = input("genre: ").strip() or "Action"
    publisher = input("publisher: ").strip() or "Example Studios"

    def fprompt(label):
        try:
            return float(input(f"{label} sales (e.g., 0): ").strip() or "0")
        except:
            return 0.0

    sales = {
        "na": fprompt("NA"),
        "eu": fprompt("EU"),
        "jp": fprompt("JP"),
        "other": fprompt("Other"),
    }
    sales["global"] = sum(sales.values())

    doc = {
        "name": name,
        "platform": platform,
        "year": year,
        "genre": genre,
        "publisher": publisher,
        "sales": sales,
        "regions": [
            {"region": "NA", "sales": sales["na"]},
            {"region": "EU", "sales": sales["eu"]},
            {"region": "JP", "sales": sales["jp"]},
            {"region": "Other", "sales": sales["other"]},
        ]
    }
    res = coll.insert_one(doc)
    print("Inserted _id:", res.inserted_id)

def update_game():
    coll = connect()
    name = input("Update which game by exact name?: ").strip()
    field = input("Field to update (e.g., sales.global or publisher): ").strip() or "sales.global"
    value = input("New value (numbers will be cast): ").strip()

    # naive type casting
    try:
        if "." in value:
            new_val = float(value)
        else:
            new_val = int(value)
    except:
        new_val = value

    updated = coll.find_one_and_update(
        {"name": name},
        {"$set": {field: new_val}},
        return_document=ReturnDocument.AFTER
    )
    if updated:
        print("Updated document:")
        pp.pprint(updated)
    else:
        print("No document matched.")

def delete_game():
    coll = connect()
    name = input("Delete which game by exact name?: ").strip()
    res = coll.delete_one({"name": name})
    print(f"Deleted count: {res.deleted_count}")

MENU = """
Choose an option:
  1) INSERT a new game
  2) UPDATE a game
  3) DELETE a game
  q) Quit
"""

def main():
    print(MENU)
    coll = connect()
    coll.database.command("ping")
    while True:
        choice = input("Enter choice: ").strip().lower()
        if choice == "1":
            insert_game()
        elif choice == "2":
            update_game()
        elif choice == "3":
            delete_game()
        elif choice in ("q", "quit", "exit"):
            print("Bye.")
            break
        else:
            print(MENU)

if __name__ == "__main__":
    main()
