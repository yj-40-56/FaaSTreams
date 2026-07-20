#!/usr/bin/env python3
import asyncio
import csv
import math
import os
import statistics
import sys
import time

import aiohttp


# Edit these defaults or override them with same-named environment variables.
COMPRESSION = float(os.getenv("COMPRESSION", "15"))
DAY_START_HOUR = float(os.getenv("DAY_START_HOUR", "6"))
ACTIVE_HOURS = float(os.getenv("ACTIVE_HOURS", "19"))
SHUTDOWN_HOURS = float(os.getenv("SHUTDOWN_HOURS", "5"))
PEAK_RATE = float(os.getenv("PEAK_RATE", "40"))

REAL_HOUR_SECONDS = 3600
OUTPUT = os.getenv("OUTPUT", "results_trainday_c8.csv")


def wall_seconds(real_hours):
    return real_hours * REAL_HOUR_SECONDS / COMPRESSION


ACTIVE_WALL_SECONDS = wall_seconds(ACTIVE_HOURS)
SHUTDOWN_WALL_SECONDS = wall_seconds(SHUTDOWN_HOURS)
RUN_WALL_SECONDS = ACTIVE_WALL_SECONDS + SHUTDOWN_WALL_SECONDS


def traffic_rate(real_hour_unwrapped):
    """Linearly interpolate the camel-back weekday transit demand curve."""
    anchors = [
        (6.0, 6),
        (7.0, 22),
        (8.0, 40),
        (9.0, 32),
        (10.0, 18),
        (11.0, 14),
        (12.0, 16),
        (13.0, 15),
        (14.0, 14),
        (15.0, 18),
        (16.0, 30),
        (17.0, 40),
        (18.0, 38),
        (19.0, 24),
        (20.0, 15),
        (21.0, 11),
        (22.0, 8),
        (23.0, 6),
        (24.0, 4),  # 00:00 on the following day
        (25.0, 4),  # hold until the hard shutdown at 01:00
    ]
    for (left_hour, left_rate), (right_hour, right_rate) in zip(anchors, anchors[1:]):
        if left_hour <= real_hour_unwrapped <= right_hour:
            fraction = (real_hour_unwrapped - left_hour) / (right_hour - left_hour)
            nominal_rate = left_rate + fraction * (right_rate - left_rate)
            return nominal_rate * PEAK_RATE / 40.0
    return 0.0


def build_schedule():
    if COMPRESSION <= 0 or ACTIVE_HOURS < 0 or SHUTDOWN_HOURS < 0:
        raise ValueError("COMPRESSION must be positive and hour counts non-negative")
    if not math.isclose(ACTIVE_HOURS + SHUTDOWN_HOURS, 24.0):
        raise ValueError("ACTIVE_HOURS + SHUTDOWN_HOURS must equal 24")

    offsets = []
    carry = 0.0
    active_seconds = int(round(ACTIVE_WALL_SECONDS))
    for second in range(active_seconds):
        real_hour = DAY_START_HOUR + second * COMPRESSION / REAL_HOUR_SECONDS
        carry += traffic_rate(real_hour)
        count = int(carry)
        carry -= count
        rate = traffic_rate(real_hour)
        offsets.extend(second + index / rate for index in range(count))

    assert all(0 <= offset < ACTIVE_WALL_SECONDS for offset in offsets)
    assert not any(ACTIVE_WALL_SECONDS <= offset < RUN_WALL_SECONDS for offset in offsets)
    return offsets


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
    lower, upper = math.floor(rank), math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def is_rush(offset):
    real_hour = DAY_START_HOUR + offset * COMPRESSION / REAL_HOUR_SECONDS
    return abs(real_hour - 8.0) <= 0.5 or abs(real_hour - 17.0) <= 0.5


async def run(url):
    offsets = build_schedule()
    print(f"Scheduled requests: {len(offsets)}")
    print(f"Run duration: {RUN_WALL_SECONDS / 60:.1f} wall minutes")
    print(
        f"Shutdown: {SHUTDOWN_WALL_SECONDS / 60:.1f} wall minutes with zero sends "
        f"({(DAY_START_HOUR + ACTIVE_HOURS) % 24:02.0f}:00-"
        f"{DAY_START_HOUR % 24:02.0f}:00 simulated time)"
    )

    timeout = aiohttp.ClientTimeout(total=180)
    connector = aiohttp.TCPConnector(limit=0)
    start_time = time.perf_counter()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [send_one(session, url, offset, start_time) for offset in offsets]
        results = await asyncio.gather(*tasks)

    results.sort(key=lambda row: row["scheduled_s"])
    with open(OUTPUT, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    # Checkpoint request data before the silent interval, then keep the driver
    # alive for the complete compressed day so the run really lasts 96 minutes.
    remaining = start_time + RUN_WALL_SECONDS - time.perf_counter()
    if remaining > 0:
        print(
            f"Checkpointed {OUTPUT}; shutdown in progress: "
            f"waiting {remaining / 60:.1f} minutes with zero sends",
            flush=True,
        )
        await asyncio.sleep(remaining)

    successful = [row["latency_ms"] for row in results if row["status"] == 200]
    lags = [row["send_s"] - row["scheduled_s"] for row in results]
    rush_lags = [
        row["send_s"] - row["scheduled_s"]
        for row in results
        if is_rush(row["scheduled_s"])
    ]
    print(f"Wrote {OUTPUT}; ok/total: {len(successful)}/{len(results)}")
    if successful:
        print(
            "200 latency ms: "
            f"p50={statistics.median(successful):.1f}, "
            f"p95={percentile(successful, 95):.1f}, max={max(successful):.1f}"
        )
    print(
        "Send lag s: "
        f"overall p95={percentile(lags, 95):.3f}, max={max(lags):.3f}; "
        f"rush p95={percentile(rush_lags, 95):.3f}, max={max(rush_lags):.3f}"
    )
    if max(rush_lags) > 1.0:
        print(
            "WARNING: rush-hour sends drifted over 1s behind schedule. The client "
            "flattened the peaks; lower PEAK_RATE or run the driver on a same-region VM."
        )
    else:
        print("Integrity check: rush-hour sends stayed within 1s of schedule.")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 train_day.py <URL>")
    asyncio.run(run(sys.argv[1].rstrip("/")))


if __name__ == "__main__":
    main()
