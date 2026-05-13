# Worker

FaaS worker that fetches a range of AIS records from Redis, loads them into DuckDB, and emits proximity warnings for vessels approaching defined hazard zones.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running

Make sure the Redis mock stack is running first (see `docker/` in the repo root).

```bash
python handler.py '{"start_index": 0, "end_index": 999}'
```

`start_index` and `end_index` are inclusive and correspond to the `ais:<index>` keys in Redis.
