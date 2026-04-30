import csv
import os

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_FILE = "/app/data/ais.csv"
KEY_PREFIX = "ais:"
BATCH_SIZE = 500

# map csv column names to clean keys
FIELD_MAP = {
    "# Timestamp": "timestamp",
    "Type of mobile": "typeOfMobile",
    "MMSI": "mmsi",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Navigational status": "navigationalStatus",
    "ROT": "rot",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "IMO": "imo",
    "Callsign": "callsign",
    "Name": "name",
    "Ship type": "shipType",
    "Cargo type": "cargoType",
    "Width": "width",
    "Length": "length",
    "Type of position fixing device": "positionFixingDevice",
    "Draught": "draught",
    "Destination": "destination",
    "ETA": "eta",
    "Data source type": "dataSourceType",
    "A": "a",
    "B": "b",
    "C": "c",
    "D": "d",
}

def import_data(r: redis.Redis) -> None:
    total = 0
    pipe = r.pipeline(transaction=False)

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            record = {
                clean_key: row.get(csv_key, "")
                for csv_key, clean_key in FIELD_MAP.items()
            }

            pipe.hset(f"{KEY_PREFIX}{i}", mapping=record)
            total += 1

            if total % BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                print(f"  Imported {total} records...")

    pipe.execute()

    r.set("ais:total", total)
    print(f"Done. Imported {total} records.")


if __name__ == "__main__":
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("Connected.")
    print(f"Importing data from {DATA_FILE}...")
    import_data(r)
