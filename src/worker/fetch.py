import os

from flask import json
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def fetch_range(start_index: int, end_index: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline(transaction=False)
    for i in range(start_index, end_index + 1):
        pipe.hgetall(f"ais:{i}")
    return [record for record in pipe.execute() if record]

def fetch_by_timestamp(start_ts: int, end_ts: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    raw_data = r.zrangebyscore("mod-stream", start_ts - 1, end_ts + 1)
    return [json.loads(record) for record in raw_data]