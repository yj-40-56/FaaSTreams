import json
import os
import json
import threading
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_KEY_PREFIX = "data"  # matches ingestor's dataKey const (cmd/coordinator/ingestor/main.go)
PENDING_KEY_PREFIX = "pending"  # matches windower's pendingKey const (src/windower/main.go)
INFLIGHT_KEY_PREFIX = "inflight"  # matches windower's inflightKey const (src/windower/main.go)

# Lease timings.
#
# The lease must outlive several missed refreshes -- this process may be under
# heavy memory pressure while the query runs -- but expire soon enough that a
# window whose worker died is retried quickly. The ratio is what matters: a TTL
# of four refresh intervals gives four chances to survive a stall.
#
# Like the windower's timing consts these are absolute, and only hold while the
# window size stays in the same order of magnitude. A 5s window processed under
# a 120s lease is held far longer than it took to fill; deriving both ends from
# the window size is the next step.
LEASE_TTL_SECONDS = 120
LEASE_REFRESH_SECONDS = 30


def _pending_member(query_name: str, window_id: str, window_start: int, window_end: int) -> str:
    """Contract shared with src/windower/main.go:pendingMember."""
    return f"{query_name}:{window_id}:{window_start}:{window_end}"


class Lease:
    """Tells the windower this window is being worked on, not lost.

    The windower retries a pending window only when no lease exists for it, so
    a query that runs longer than the retry backstop can never be handed to a
    second worker. If this process dies the key expires on its own and the
    window is retried promptly instead of waiting out a fixed timeout.
    """

    def __init__(self, data_source: str, member: str):
        self._key = f"{INFLIGHT_KEY_PREFIX}:{data_source}:{member}"
        self._redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._refresh, daemon=True)

    def start(self) -> "Lease":
        self._redis.set(self._key, "1", ex=LEASE_TTL_SECONDS)
        self._thread.start()
        print(f"[Lease] Holding '{self._key}' (ttl={LEASE_TTL_SECONDS}s)", flush=True)
        return self

    def _refresh(self) -> None:
        while not self._stop.wait(LEASE_REFRESH_SECONDS):
            try:
                self._redis.set(self._key, "1", ex=LEASE_TTL_SECONDS)
            except redis.RedisError as e:
                # Worth knowing about: enough of these in a row and the
                # windower will conclude this worker died.
                print(f"[Lease] Failed to refresh '{self._key}': {e}", flush=True)

    def release(self) -> None:
        self._stop.set()
        try:
            self._redis.delete(self._key)
        except redis.RedisError as e:
            print(f"[Lease] Failed to release '{self._key}': {e}", flush=True)


def start_lease(data_source: str, query_name: str, window_id: str,
                window_start: int, window_end: int) -> Lease:
    return Lease(data_source, _pending_member(query_name, window_id, window_start, window_end)).start()

def fetch_window(window_start: int, window_end: int, data_source: str) -> list[dict]:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    key = f"{DATA_KEY_PREFIX}:{data_source}"
    members = r.zrangebyscore(key, window_start, window_end)
    print(f"[Fetch] Found {len(members)} member(s) in '{key}' for {window_start}-{window_end}", flush=True)
    return [json.loads(m) for m in members]

def clear_pending(data_source: str, query_name: str, window_id: str,
                  window_start: int, window_end: int) -> None:
    """Release the windower's claim on this window.

    Until this runs, `pending:<source>` holds the window's start score and the
    windower refuses to prune any event below it -- so a window that never
    reaches here keeps its data alive for a retry instead of being deleted.
    The member format is a contract shared with
    src/windower/main.go:pendingMember.
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    key = f"{PENDING_KEY_PREFIX}:{data_source}"
    member = _pending_member(query_name, window_id, window_start, window_end)
    removed = r.zrem(key, member)
    r.hdel(f"{PENDING_KEY_PREFIX}:meta:{data_source}", member)
    print(f"[Fetch] Cleared pending '{member}' from '{key}' (removed={removed})", flush=True)

def delete_window(window_start: int, window_end: int, data_source: str) -> None:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    key = f"{DATA_KEY_PREFIX}:{data_source}"
    r.zremrangebyscore(key, window_start, window_end)
    print(f"Deleted window {window_start} - {window_end} from Redis key '{key}'", flush=True)
