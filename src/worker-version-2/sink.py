"""
sink.py

Same logic as the original `_forward_to_sink`, just pulled into its own
module so main.py's orchestration doesn't have HTTP details mixed into it,
and so it can be swapped for a fake in tests.
"""

import json
import os
import urllib.error
import urllib.request

DATA_SINK_URL = os.getenv("DATA_SINK_URL")


def forward(results: list[dict], window_start: int, window_end: int,
            query: str, query_name: str, return_type: str) -> None:
    if not DATA_SINK_URL:
        print("DATA_SINK_URL not set, skipping data sink.", flush=True)
        return

    payload = json.dumps({
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query": query,
        "query_name": query_name,
        "return_type": return_type,
    }).encode()

    req = urllib.request.Request(
        DATA_SINK_URL, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print("Forwarded results to data sink.", flush=True)
    except urllib.error.URLError as exc:
        print(f"Failed to forward to data sink: {exc}", flush=True)