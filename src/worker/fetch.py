import json
import os
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_KEY_PREFIX = "data"  # matches ingestor's dataKey const (cmd/coordinator/ingestor/main.go)

def fetch_window(window_start: int, window_end: int, data_source: str) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    key = f"{DATA_KEY_PREFIX}:{data_source}"
    # Windowing uses [start, end). Redis ranges are inclusive by default, so
    # make the upper bound exclusive to avoid processing boundary events twice.
    members = r.zrangebyscore(key, window_start, f"({window_end}")
    print(f"[Fetch] Found {len(members)} member(s) in '{key}' for {window_start}-{window_end}", flush=True)
    return [json.loads(m) for m in members]

def delete_window(window_start: int, window_end: int, data_source: str) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    key = f"{DATA_KEY_PREFIX}:{data_source}"
    r.zremrangebyscore(key, window_start, window_end)
    print(f"Deleted window {window_start} - {window_end} from Redis key '{key}'", flush=True)
