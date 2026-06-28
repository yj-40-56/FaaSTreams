import os
import redis

host = os.environ.get("REDIS_HOST", "10.101.64.19")
port = int(os.environ.get("REDIS_PORT", 6379))
env = os.environ.get("ENV", "baseline")

patterns = [
    f"coordinator-{env}*",
    f"mod-stream-{env}*",
]

r = redis.Redis(host=host, port=port)
total = 0
for pattern in patterns:
    keys = r.keys(pattern.encode())
    print(f"  Pattern '{pattern}': {len(keys)} keys")
    if keys:
        r.delete(*keys)
        total += len(keys)

print(f"Deleted {total} keys total")
