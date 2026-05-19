import os
import json
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

def fetch_window(window_start: int, window_end: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    results = r.zrangebyscore("mod-stream", window_start, window_end)
    
    records = []
    for result in results:
        record = json.loads(result)
        records.append(record)
    
    return records

def delete_window(window_start: int, window_end: int) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.zremrangebyscore("mod-stream", window_start, window_end)
    print(f"Deleted window {window_start} - {window_end} from Redis", flush=True)