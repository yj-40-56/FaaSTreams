import json
import os
import json
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_KEY", "mod-stream")

def fetch_window(window_start: int, window_end: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    members = r.zrangebyscore(REDIS_KEY, window_start, window_end)
    print(f"[Fetch] Found {len(members)} member(s) in '{REDIS_KEY}' for {window_start}-{window_end}", flush=True)
    return [json.loads(m) for m in members]

def delete_window(window_start: int, window_end: int) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.zremrangebyscore(REDIS_KEY, window_start, window_end)
    print(f"Deleted window {window_start} - {window_end} from Redis", flush=True)
