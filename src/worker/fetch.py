import os

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def fetch_range(start_index: int, end_index: int) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline(transaction=False)
    for i in range(start_index, end_index + 1):
        pipe.hgetall(f"ais:{i}")
    return [record for record in pipe.execute() if record]
