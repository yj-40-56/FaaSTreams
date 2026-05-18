import json
import os

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def fetch_range(window_start: int, window_end: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    members = r.zrangebyscore("mod-stream", window_start, window_end)
    return [json.loads(m) for m in members]
