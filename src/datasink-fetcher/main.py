import json
import os
import redis
import functions_framework

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

@functions_framework.http
def handler(request):
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    args = request.args
    score_min = float(args.get("from", "-inf"))
    score_max = float(args.get("to", "+inf"))

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    raw_entries = r.zrangebyscore("analytics-results", score_min, score_max)

    results = []
    for entry in raw_entries:
        try:
            results.append(json.loads(entry))
        except json.JSONDecodeError:
            continue

    return (json.dumps({"data": results, "count": len(results)}), 200, CORS_HEADERS)