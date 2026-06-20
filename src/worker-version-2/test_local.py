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

REDIS_KEY = os.getenv("REDIS_KEY", "mod-stream")
RESULTS_PATH = Path("./worker_test_results.json")
CONFIG_DIR = Path(__file__).parent / "configurations"
QUERIES_FILE = CONFIG_DIR / "queries.yml"
DOMAIN_FILE = CONFIG_DIR / "domain.yml"
ZONES_FILE = CONFIG_DIR / "zones.json"

os.environ.setdefault("DOMAIN_FIELD_FILE", str(DOMAIN_FILE))
os.environ.setdefault("ZONES_FILE", str(ZONES_FILE))

import main as worker_main


QUERY_NAMES = [
    "zone_proximity_alerts",
    "report_rate_per_object",
    "trajectory_path",
]


def load_queries() -> dict[str, dict]:
    with open(QUERIES_FILE) as f:
        data = yaml.safe_load(f) or {}
    return {q["name"]: q for q in data.get("queries", [])}


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
    print(f"Query {query_name} produced {len(results)} result(s) and would forward to sink.")


def fake_fetch_factory(records: list[dict]):
    def _fetch(window_start, window_end):
        print("Using fake fetch (in-memory records)")
        return records
    return _fetch


def main():
    queries = load_queries()
    query_names = QUERY_NAMES

    # Try seeding Redis; if that fails, fallback to fake fetch
    try:
        window_start, window_end = seed_redis(SAMPLE_RECORDS, REDIS_KEY)
        fetch_fn = None  # use real fetch in main.process
        print(f"Seeded Redis {len(SAMPLE_RECORDS)} records scores {window_start}-{window_end}")
    except Exception as exc:
        print(f"Redis seed failed: {exc}; falling back to in-memory fetch.")
        fetch_fn = fake_fetch_factory(SAMPLE_RECORDS)
        window_start, window_end = 0, 9999999999

    all_runs = []
    for query_name in query_names:
        query_config = queries.get(query_name)
        if query_config is None:
            raise KeyError(f"Query {query_name} not found in {QUERIES_FILE}")

        print(f"Running query {query_name} (return_type={query_config.get('return_type')})")
        # Only pass fetch_fn override when using the in-memory fake; when
        # fetch_fn is None the worker's default (real Redis fetch) will be
        # used.
        if fetch_fn is None:
            result = worker_main.process(
                window_start,
                window_end,
                query_config["query"],
                query_name,
                query_config.get("return_type", "unknown"),
                sink_fn=file_sink,
            )
        else:
            result = worker_main.process(
                window_start,
                window_end,
                query_config["query"],
                query_name,
                query_config.get("return_type", "unknown"),
                fetch_fn=fetch_fn,
                sink_fn=file_sink,
            )
        all_runs.append(result)

    output = {
        "window_start": window_start,
        "window_end": window_end,
        "runs": all_runs,
    }
    RESULTS_PATH.write_text(json.dumps(output, indent=2))
    print(f"Wrote combined results to {RESULTS_PATH}")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()