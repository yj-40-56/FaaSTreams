#!/usr/bin/env python3
import asyncio
import csv
import math
import statistics
import sys
import time

import aiohttp


BURST_RATE = 60
BASE_RATE = 2
BURST_SEC = 30
GAP_SEC = 90
CYCLES = 3

TOTAL_DURATION = (BURST_SEC + GAP_SEC) * CYCLES
VOLATILE_REQUESTS = (BURST_RATE * BURST_SEC + BASE_RATE * GAP_SEC) * CYCLES
STEADY_RATE = VOLATILE_REQUESTS / TOTAL_DURATION


def phase_offsets(start_s, duration_s, rate):
    count = int(duration_s * rate)
    assert count == duration_s * rate
    return [start_s + index / rate for index in range(count)]


def schedules():
    volatile = []
    cycle_sec = BURST_SEC + GAP_SEC
    for cycle in range(CYCLES):
        start = cycle * cycle_sec
        volatile.extend(phase_offsets(start, BURST_SEC, BURST_RATE))
        volatile.extend(phase_offsets(start + BURST_SEC, GAP_SEC, BASE_RATE))

    steady = phase_offsets(0, TOTAL_DURATION, STEADY_RATE)
    assert len(steady) == len(volatile) == VOLATILE_REQUESTS
    assert TOTAL_DURATION == cycle_sec * CYCLES
    return {"steady": steady, "volatile": volatile}


async def send_one(session, url, scheduled_s, start_time):
    await asyncio.sleep(max(0.0, start_time + scheduled_s - time.perf_counter()))
    send_s = time.perf_counter() - start_time
    status = 0
    try:
        async with session.get(url) as response:
            await response.read()
            status = response.status
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    recv_s = time.perf_counter() - start_time
    return {
        "scheduled_s": scheduled_s,
        "send_s": send_s,
        "recv_s": recv_s,
        "latency_ms": (recv_s - send_s) * 1000.0,
        "status": status,
    }


def percentile(values, percent):
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percent / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


async def run(url, mode):
    offsets = schedules()[mode]
    timeout = aiohttp.ClientTimeout(total=180)
    connector = aiohttp.TCPConnector(limit=0)
    start_time = time.perf_counter()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [send_one(session, url, offset, start_time) for offset in offsets]
        results = await asyncio.gather(*tasks)

    results.sort(key=lambda row: row["scheduled_s"])
    output = f"results_{mode}.csv"
    with open(output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    successful = [row["latency_ms"] for row in results if row["status"] == 200]
    print(f"Wrote {output}")
    print(f"ok/total: {len(successful)}/{len(results)}")
    if successful:
        print(
            "200 latency ms: "
            f"p50={statistics.median(successful):.1f}, "
            f"p95={percentile(successful, 95):.1f}, max={max(successful):.1f}"
        )

    lags = [row["send_s"] - row["scheduled_s"] for row in results]
    burst_lags = lags
    if mode == "volatile":
        cycle_sec = BURST_SEC + GAP_SEC
        burst_lags = [
            lag
            for row, lag in zip(results, lags)
            if row["scheduled_s"] % cycle_sec < BURST_SEC
        ]
    print(
        "send schedule lag s: "
        f"p95={percentile(burst_lags, 95):.3f}, max={max(burst_lags):.3f}"
    )
    if max(burst_lags) > 1.0:
        print(
            "WARNING: sends drifted more than 1s behind schedule during the measured "
            "load. The client may have flattened the burst, invalidating the run. "
            "Lower BURST_RATE and rerun both modes."
        )
    else:
        print("Integrity check: send timing stayed within 1s of schedule.")


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in {"steady", "volatile"}:
        raise SystemExit("Usage: python3 load_test.py <SERVICE_URL> <steady|volatile>")
    asyncio.run(run(sys.argv[1].rstrip("/"), sys.argv[2]))


if __name__ == "__main__":
    main()
