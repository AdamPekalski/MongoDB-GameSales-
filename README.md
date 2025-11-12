#########################
##PLEASE READ THIS FILE##
#########################
##PLEASE READ THIS FILE##
#########################
##PLEASE READ THIS FILE##
#########################

## Files
- schema.json | MongoDB JSON Schema validator.
- create_and_load.py | Creates DB/collection with validation and loads CSV with transformation + embedded arrays.
- queries.py | Interactive menu for required queries (all, embedded arrays, projection, sort, aggregation). Uses input() to collect values.
- crud.py | Interactive insert/update/delete using input().
- requirements.txt — Python dependencies.

## Prereqs
- Python 3.9+
- `pip install -r requirements.txt`
- MongoDB running locally (e.g., Docker): `docker run -d -p 28017:27017 --name mongo mongo:7`

Optional environment variables (defaults shown):
```bash
export MONGO_URI="mongodb://localhost:28017/"
export DB_NAME="gamesdb"
export COLL_NAME="games"
export CSV_PATH="vgsales.csv"
export SCHEMA_PATH="schema.json"
```

## Load data
Put vgsales.csv in the same folder and run:
```bash
python create_and_load.py
```

## Run queries
```bash
python queries.py
```

## CRUD operations
```bash
python crud.py
```
################################
##I hope all works as intended##
################################