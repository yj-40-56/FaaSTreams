import json
import time
import os
import redis
import functions_framework


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_KEY", "analytics-results")


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}
    try:
        results = body["results"]
        window_start = int(body["window_start"])
        window_end = int(body["window_end"])
        query_name = str(body.get("query_name"))
        return_type = str(body["return_type"])
        ingestor_start = body.get("ingestor_start", 0)
        ingestor_end   = body.get("ingestor_end", 0)
        windower_start = body.get("windower_start", 0)
        windower_end   = body.get("windower_end", 0)
        worker_start   = body.get("worker_start", 0)
        worker_end     = body.get("worker_end", 0)
    except (KeyError, TypeError, ValueError) as e:
        print(f"[DataSink] Invalid payload: {e} | body={body}", flush=True)
        return {"error": f"Invalid payload: {e}"}, 400

    log_prefix = f"[DataSink:{query_name}]"

    start_human = time.strftime("%H:%M:%S", time.gmtime(window_start))
    end_human = time.strftime("%H:%M:%S", time.gmtime(window_end))
    print(f"{log_prefix} Received {len(results)} result(s) for window {start_human} - {end_human} ({window_start}-{window_end})", flush=True)

    payload = json.dumps({
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query": query_name,
        "return_type": return_type,
        "ingestor_start": ingestor_start,  # t0
        "ingestor_end":   ingestor_end,    # t1
        "windower_start": windower_start,  # t2
        "windower_end":   windower_end,    # t3
        "worker_start":   worker_start,    # t4
        "worker_end":     worker_end,      # t5
    })

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        score = time.time()
        r.zadd(REDIS_KEY, {payload: score})
    except redis.RedisError as e:
        print(f"{log_prefix} ERROR: Failed to store results in Redis: {e}", flush=True)
        return {"error": str(e)}, 500

    print(f"{log_prefix} Stored {len(results)} results in '{REDIS_KEY}' at score {score:.0f}.", flush=True)
    return {"stored": len(results)}
