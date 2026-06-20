"""
test_local.py

Seed Redis (if available) with a small set of raw records and run the
worker-version-2 orchestration (`main.process`) against that window.

Usage:
  python test_local.py
"""

import json
import os
import time
from pathlib import Path

import yaml

import main as worker_main

# Config-aware defaults matching the module under test
REDIS_KEY = os.getenv("REDIS_KEY", "mod-stream")
RESULTS_PATH = Path("./worker_test_results.json")
QUERIES_FILE = Path(__file__).parent / "configurations/queries.yml"


def load_query(name: str) -> str:
    with open(QUERIES_FILE) as f:
        data = yaml.safe_load(f) or {}
    for q in data.get("queries", []):
        if q.get("name") == name:
            return q.get("query")
    raise KeyError(f"Query {name} not found in {QUERIES_FILE}")


SAMPLE_RECORDS = [
    {
        "MMSI": "111111111",
        "Latitude": 55.6,
        "Longitude": 7.6,
        "# Timestamp": "2026-06-20T12:00:00Z",
        "SOG": 5.2,
        "heading": 90,
        "shipType": "Cargo",
        "Navigational status": "Under way"
    },
    {
        "MMSI": "111111111",
        "Latitude": 55.61,
        "Longitude": 7.61,
        "# Timestamp": "2026-06-20T12:00:10Z",
        "SOG": 5.0,
        "heading": 92,
        "shipType": "Cargo",
        "Navigational status": "Under way"
    },
    {
        "MMSI": "222222222",
        "Latitude": 55.62,
        "Longitude": 7.62,
        "# Timestamp": "2026-06-20T12:00:05Z",
        "SOG": 0.0,
        "heading": 0,
        "shipType": "Tanker",
        "Navigational status": "At anchor"
    }
]


def seed_redis(records: list[dict], key: str) -> tuple[int, int]:
    import redis

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    r = redis.Redis(host=host, port=port, decode_responses=True)
    now = int(time.time())
    scores = []
    for i, rec in enumerate(records):
        score = now + i
        r.zadd(key, {json.dumps(rec): score})
        scores.append(score)
    return min(scores), max(scores)


def file_sink(results, window_start, window_end, query, query_name, return_type):
    out = {
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query": query,
        "query_name": query_name,
        "return_type": return_type,
    }
    with open(RESULTS_PATH, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Wrote results to {RESULTS_PATH}")


def fake_fetch_factory(records: list[dict]):
    def _fetch(window_start, window_end):
        print("Using fake fetch (in-memory records)")
        return records
    return _fetch


def main():
    # Choose a query present in queries.yml that doesn't require zones
    query_name = "report_rate_per_object"
    query = load_query(query_name)

    # Try seeding Redis; if that fails, fallback to fake fetch
    try:
        window_start, window_end = seed_redis(SAMPLE_RECORDS, REDIS_KEY)
        fetch_fn = None  # use real fetch in main.process
        print(f"Seeded Redis {len(SAMPLE_RECORDS)} records scores {window_start}-{window_end}")
    except Exception as exc:
        print(f"Redis seed failed: {exc}; falling back to in-memory fetch.")
        fetch_fn = fake_fetch_factory(SAMPLE_RECORDS)
        window_start, window_end = 0, 9999999999

    kwargs = {"sink_fn": file_sink}
    if fetch_fn is not None:
        kwargs["fetch_fn"] = fetch_fn

    # Run the worker orchestration, writing sink output to local file
    result = worker_main.process(
        window_start,
        window_end,
        query,
        query_name,
        "temporal",
        **kwargs,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()