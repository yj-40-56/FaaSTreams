import csv
import os
import json
import time
from datetime import datetime
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_FILE = "/app/data/ais.csv"
BATCH_SIZE = 500

FIELD_MAP = {
    "# Timestamp": "# Timestamp",
    "Type of mobile": "typeOfMobile",
    "MMSI": "MMSI",
    "Latitude": "Latitude",
    "Longitude": "Longitude",
    "Navigational status": "Navigational status",
    "ROT": "rot",
    "SOG": "SOG",
    "COG": "cog",
    "Heading": "heading",
    "IMO": "imo",
    "Callsign": "callsign",
    "Name": "Name",
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

def parse_timestamp(ts_str: str) -> float:
    dt = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
    return dt.timestamp()

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
            try:
                score = parse_timestamp(row["# Timestamp"])
            except (ValueError, KeyError):
                continue

            pipe.zadd("mod-stream", {json.dumps(record): score})
            total += 1
            if total % BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                print(f"  Imported {total} records...")
    pipe.execute()
    print(f"Done. Imported {total} records.")

if __name__ == "__main__":
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("Connected.")
    print(f"Importing data from {DATA_FILE}...")
    import_data(r)