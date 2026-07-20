#!/usr/bin/env python3
import csv
import math
from collections import defaultdict

import matplotlib.pyplot as plt


def percentile(values, percent):
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percent / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def load_buckets(mode):
    requests = defaultdict(int)
    latencies = defaultdict(list)
    with open(f"results_{mode}.csv", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            second = int(float(row["send_s"]))
            requests[second] += 1
            if int(row["status"]) == 200:
                latencies[second].append(float(row["latency_ms"]))

    last_second = max(requests)
    seconds = list(range(last_second + 1))
    rps = [requests[second] for second in seconds]
    p95 = [
        percentile(latencies[second], 95) if latencies[second] else math.nan
        for second in seconds
    ]
    return seconds, rps, p95


def main():
    figure, (rate_axis, latency_axis) = plt.subplots(
        2, 1, figsize=(12, 7), sharex=True, constrained_layout=True
    )
    colors = {"steady": "#2673b8", "volatile": "#e07020"}
    for mode in ("steady", "volatile"):
        seconds, rps, p95 = load_buckets(mode)
        rate_axis.plot(seconds, rps, label=mode, color=colors[mode], linewidth=1.5)
        latency_axis.plot(seconds, p95, label=mode, color=colors[mode], linewidth=1.5)

    rate_axis.set_ylabel("Requests sent / second")
    rate_axis.set_title("Equal-volume Cloud Run load: steady vs volatile")
    rate_axis.legend()
    rate_axis.grid(alpha=0.25)
    latency_axis.set_xlabel("Seconds from test start")
    latency_axis.set_ylabel("p95 latency (ms)")
    latency_axis.legend()
    latency_axis.grid(alpha=0.25)
    figure.savefig("load_comparison.png", dpi=180)
    print("Wrote load_comparison.png")


if __name__ == "__main__":
    main()
