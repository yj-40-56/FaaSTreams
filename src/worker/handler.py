import os
import time

import functions_framework


@functions_framework.http
def handler(request):
    """Burn a fixed amount of CPU and return a small successful response."""
    try:
        work_ms = max(0.0, float(os.getenv("WORK_MS", "300")))
    except ValueError:
        work_ms = 300.0

    started = time.perf_counter()
    deadline = started + work_ms / 1000.0
    iterations = 0
    while time.perf_counter() < deadline:
        iterations += 1

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {
        "ok": True,
        "work_ms": work_ms,
        "elapsed_ms": round(elapsed_ms, 3),
        "iterations": iterations,
    }, 200
