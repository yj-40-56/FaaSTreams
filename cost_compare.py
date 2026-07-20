#!/usr/bin/env python3
import csv
import math
import os
from collections import deque


COMPRESSION = float(os.getenv("COMPRESSION", "15"))
ACTIVE_HOURS = float(os.getenv("ACTIVE_HOURS", "19"))
SHUTDOWN_HOURS = float(os.getenv("SHUTDOWN_HOURS", "5"))
WORK_MS = float(os.getenv("WORK_MS", "300"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
COOLDOWN_REAL_SECONDS = float(os.getenv("COOLDOWN_REAL_SECONDS", "900"))
VCPU = float(os.getenv("VCPU", "1"))
MEMORY_GIB = float(os.getenv("MEMORY_GIB", "0.5"))
VCPU_SECOND_PRICE = float(os.getenv("VCPU_SECOND_PRICE", "0.000024"))
GIB_SECOND_PRICE = float(os.getenv("GIB_SECOND_PRICE", "0.0000025"))
REQUEST_MILLION_PRICE = float(os.getenv("REQUEST_MILLION_PRICE", "0.40"))

PEAK_RATE = float(os.getenv("PEAK_RATE", "40"))
VCPU_PER_VM = int(os.getenv("VCPU_PER_VM", "4"))
VM_HOURLY = float(os.getenv("VM_HOURLY", "0.134"))
INPUT = os.getenv("INPUT", "results_trainday_c8.csv")

PEAK_CONCURRENCY = PEAK_RATE * WORK_MS / 1000.0
REQUIRED_VCPUS = math.ceil(PEAK_CONCURRENCY)
FLINK_VMS = math.ceil(REQUIRED_VCPUS / VCPU_PER_VM)
TOTAL_WALL_SECONDS = round((ACTIVE_HOURS + SHUTDOWN_HOURS) * 3600 / COMPRESSION)
COOLDOWN_WALL_SECONDS = math.ceil(COOLDOWN_REAL_SECONDS / COMPRESSION)


def load_rows():
    with open(INPUT, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def instance_series(rows):
    """Estimate instances from average in-flight requests in each wall-second."""
    overlap = [0.0] * TOTAL_WALL_SECONDS
    for row in rows:
        start = max(0.0, float(row["send_s"]))
        end = min(float(row["recv_s"]), TOTAL_WALL_SECONDS)
        if end <= start:
            continue
        first = max(0, math.floor(start))
        last = min(TOTAL_WALL_SECONDS, math.ceil(end))
        for second in range(first, last):
            overlap[second] += max(0.0, min(end, second + 1) - max(start, second))

    base = [math.ceil(value / CONCURRENCY) if value > 0 else 0 for value in overlap]

    # A provisioned level remains billable until its last use is outside the
    # cooldown window. This sliding maximum is conservative and auditable.
    with_tail = []
    candidates = deque()
    for second, instances in enumerate(base):
        while candidates and candidates[-1][1] <= instances:
            candidates.pop()
        candidates.append((second, instances))
        cutoff = second - COOLDOWN_WALL_SECONDS
        while candidates and candidates[0][0] <= cutoff:
            candidates.popleft()
        with_tail.append(candidates[0][1])
    return overlap, base, with_tail


def cost_from_instance_seconds(instance_wall_seconds, requests):
    instance_real_seconds = instance_wall_seconds * COMPRESSION
    vcpu_seconds = instance_real_seconds * VCPU
    gib_seconds = instance_real_seconds * MEMORY_GIB
    compute_cost = vcpu_seconds * VCPU_SECOND_PRICE
    memory_cost = gib_seconds * GIB_SECOND_PRICE
    request_cost = requests / 1_000_000 * REQUEST_MILLION_PRICE
    return {
        "requests": requests,
        "vcpu_seconds": vcpu_seconds,
        "gib_seconds": gib_seconds,
        "compute_cost": compute_cost,
        "memory_cost": memory_cost,
        "request_cost": request_cost,
        "total": compute_cost + memory_cost + request_cost,
    }


def measured_cloud_run_cost(active_hours=ACTIVE_HOURS):
    rows = load_rows()
    _, base, with_tail = instance_series(rows)
    base_wall_seconds = sum(base)
    tail_extra_wall_seconds = sum(with_tail) - base_wall_seconds
    scale = active_hours / ACTIVE_HOURS
    scenario_wall_seconds = base_wall_seconds * scale + tail_extra_wall_seconds
    cost = cost_from_instance_seconds(scenario_wall_seconds, len(rows) * scale)
    cost.update(
        {
            "base_wall_seconds": base_wall_seconds,
            "tail_extra_wall_seconds": tail_extra_wall_seconds,
            "peak_instances": max(with_tail),
            "http_429s": sum(int(row["status"]) == 429 for row in rows),
            "non_200s": sum(int(row["status"]) != 200 for row in rows),
        }
    )
    return cost


def flink_cost(vms):
    return vms * VM_HOURLY * 24


def saving_percent(cloud, flink):
    return (flink - cloud) / flink * 100.0


def main():
    cloud = measured_cloud_run_cost()
    flink = flink_cost(FLINK_VMS)
    saving = saving_percent(cloud["total"], flink)

    print("CLOUD RUN CONCURRENCY-8 ESTIMATE")
    print(f"Requests: {cloud['requests']:.0f}; HTTP 429s: {cloud['http_429s']}; non-200s: {cloud['non_200s']}")
    print(f"Peak estimated instances: {cloud['peak_instances']} (c1 observed peak: 15)")
    print(
        f"Method: average in-flight requests per 1s wall bucket, then "
        f"instances=ceil(in-flight/{CONCURRENCY})."
    )
    print(
        f"Warm tail: {COOLDOWN_REAL_SECONDS / 60:g} simulated real minutes = "
        f"{COOLDOWN_WALL_SECONDS}s wall time at {COMPRESSION:g}x compression; "
        "each provisioned level persists for that window after last use."
    )
    print(
        f"Instance wall-seconds: base={cloud['base_wall_seconds']:.1f}, "
        f"tail addition={cloud['tail_extra_wall_seconds']:.1f}"
    )
    print(f"vCPU-seconds: {cloud['vcpu_seconds']:.1f}; GiB-seconds: {cloud['gib_seconds']:.1f}")
    print(
        f"Cloud Run $/day: ${cloud['total']:.4f} "
        f"(CPU ${cloud['compute_cost']:.4f}, memory ${cloud['memory_cost']:.4f}, "
        f"requests ${cloud['request_cost']:.4f})"
    )

    print("\nCONTROLLED PEAK-SIZED FLINK BASELINE (Flink not run)")
    print(
        f"{PEAK_RATE:g} req/s x {WORK_MS / 1000:g}s = {PEAK_CONCURRENCY:g} concurrent "
        f"requests -> {REQUIRED_VCPUS} vCPU -> {FLINK_VMS} x e2-standard-4"
    )
    print(f"Flink $/day: {FLINK_VMS} x ${VM_HOURLY:.3f}/hour x 24 = ${flink:.4f}")
    label = "saving" if saving >= 0 else "premium"
    print(f"Cloud Run vs Flink: {abs(saving):.1f}% {label}")
    print(
        "Outcome drivers: Cloud Run tracks the midday trough and reaches zero during the "
        "5h shutdown; Flink pays for peak-sized capacity continuously. The modeled Cloud "
        "Run warm tail prevents undercounting scale-down billing."
    )

    print("\nSENSITIVITY: Cloud Run saving vs Flink")
    print("shutdown_h  flink_vms  baseline             cloud_$/day  flink_$/day  saving")
    for shutdown_hours in (4, 5, 6):
        scenario_cloud = measured_cloud_run_cost(24 - shutdown_hours)["total"]
        for vms in (3, 4):
            scenario_flink = flink_cost(vms)
            scenario_saving = saving_percent(scenario_cloud, scenario_flink)
            baseline = "minimum for peak" if vms == 3 else "peak + headroom"
            print(
                f"{shutdown_hours:>10}  {vms:>9}  {baseline:<19}  ${scenario_cloud:>10.4f}  "
                f"${scenario_flink:>10.4f}  {scenario_saving:>6.1f}%"
            )

    print(
        "\nHONESTY NOTE: This is a client-log reconstruction, not the Cloud Billing "
        "ledger. In-flight/8 and a fixed warm tail approximate instance allocation; use "
        "Cloud Run instance-count and billing metrics for GCP-side confirmation."
    )


if __name__ == "__main__":
    main()
