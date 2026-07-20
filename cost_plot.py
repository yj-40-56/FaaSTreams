#!/usr/bin/env python3
import math
import os

import matplotlib.pyplot as plt

import cost_compare


DAY_START_HOUR = float(os.getenv("DAY_START_HOUR", "6"))
OUTPUT = "cost_over_day_c8.png"


def main():
    rows = cost_compare.load_rows()
    _, _, instances = cost_compare.instance_series(rows)
    requests_by_second = [0] * cost_compare.TOTAL_WALL_SECONDS
    for row in rows:
        second = min(int(float(row["send_s"])), len(requests_by_second) - 1)
        requests_by_second[second] += 1

    points = []
    for wall_second, instance_count in enumerate(instances):
        hour = (DAY_START_HOUR + wall_second * cost_compare.COMPRESSION / 3600.0) % 24
        resource_hourly = instance_count * (
            cost_compare.VCPU * cost_compare.VCPU_SECOND_PRICE * 3600
            + cost_compare.MEMORY_GIB * cost_compare.GIB_SECOND_PRICE * 3600
        )
        requests_per_real_hour = requests_by_second[wall_second] * 3600 / cost_compare.COMPRESSION
        request_hourly = requests_per_real_hour / 1_000_000 * cost_compare.REQUEST_MILLION_PRICE
        points.append((hour, resource_hourly + request_hourly))

    points.sort()
    hours = [point[0] for point in points]
    costs = [point[1] for point in points]
    flink_hourly = cost_compare.FLINK_VMS * cost_compare.VM_HOURLY

    figure, axis = plt.subplots(figsize=(12, 5.5), constrained_layout=True)
    axis.fill_between(hours, costs, color="#2a78b8", alpha=0.3)
    axis.plot(hours, costs, color="#2a78b8", linewidth=1.3, label="Cloud Run (c8 + warm tail)")
    axis.axhline(
        flink_hourly,
        color="#e07020",
        linewidth=2,
        label=f"Flink peak-sized ({cost_compare.FLINK_VMS} VMs, fixed 24/7)",
    )
    axis.axvspan(1, 6, color="black", alpha=0.08, label="Train shutdown")
    axis.set(
        xlim=(0, 24),
        xticks=range(0, 25, 2),
        xlabel="Simulated hour of day",
        ylabel="Cost rate ($/real hour)",
    )
    axis.set_title("Cloud Run concurrency 8 vs always-on peak-sized Flink")
    axis.grid(alpha=0.25)
    axis.legend()
    figure.savefig(OUTPUT, dpi=180)
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
