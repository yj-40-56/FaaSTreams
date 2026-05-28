import json
import time
import os
import redis
import functions_framework


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}
    try:
        results = body["results"]
        window_start = int(body["window_start"])
        window_end = int(body["window_end"])
        query = str(body["query"])
    except (KeyError, TypeError, ValueError) as e:
        return {"error": f"Invalid payload: {e}"}, 400

    payload = json.dumps({
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query": query,
    })

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    score = time.time()
    r.zadd("analytics-results", {payload: score})

    print(f"Stored {len(results)} results at score {score:.0f}.", flush=True)
    return {"stored": len(results)}
