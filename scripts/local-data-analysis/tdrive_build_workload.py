#!/usr/bin/env python3
"""Build a volatile (bursty) and matched steady (constant-rate) load-test workload from the
moving-only T-Drive stream, with burst compression decoupled from gap length.

Usage:
    python tdrive_build_workload.py [moving_only.csv] [--window-hours 2] [--repeats 4]
        [--burst-speedup 60] [--gap-wall-min 20] [--idle-scaledown-min 15]
        [--heavy-start "YYYY-MM-DD HH:MM"] [--out-dir data]

moving_only.csv defaults to data/tdrive_moving.csv

Volatile schedule: one real --window-hours window of actual T-Drive traffic (the busiest such
window in the data by event count, unless --heavy-start pins a specific start) is captured
once and replayed --repeats times as a wall-clock timeline built block by block:
  - burst block: the captured window's real inter-event timing is compressed by
    --burst-speedup (real seconds / wall seconds) - this alone sets peak amplitude.
  - gap block (skipped after the last burst): --gap-wall-min minutes of silence, specified
    directly in wall-clock time with no compression - this alone sets scale-down exposure.
Burst amplitude and gap length no longer trade off against each other through one shared
speedup, which is the point: the old design forced the speedup needed for a sharp burst to
also compress the gap below Cloud Run's idle-scaledown window.

Steady schedule: the exact same set of events (same taxi_id/lon/lat rows, same total count) as
the volatile schedule, spread at a constant rate across the same total wall-clock duration.
The two schedules differ ONLY in event timing (emit_offset_s), not in event content or count.

Writes <out-dir>/tdrive_workload_volatile.csv and <out-dir>/tdrive_workload_steady.csv
(columns: taxi_id, emit_offset_s, lon, lat - emit_offset_s is wall-clock seconds from the
start of replay), plus <out-dir>/tdrive_workload_rate.png (events/sec overlay). Prints a
report covering the idle-scaledown sanity check, peak vs. steady throughput, and which of the
two test intents (scale-to-zero cold start vs. scale-up lag only) the current settings support.
"""
import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PEAK_RATE_FLOOR = 200  # events/sec below this isn't much pressure on a single instance


def find_busiest_window(df, window_hours, resolution="5min"):
    window = pd.Timedelta(hours=window_hours)
    ts_sorted = df["ts"].values
    last_start = df["ts"].max() - window
    if last_start <= df["ts"].min():
        return df["ts"].min()

    starts = pd.date_range(df["ts"].min(), last_start, freq=resolution)
    starts_arr = starts.values
    ends_arr = starts_arr + np.timedelta64(window)

    left = np.searchsorted(ts_sorted, starts_arr, side="left")
    right = np.searchsorted(ts_sorted, ends_arr, side="left")
    counts = right - left

    best_idx = int(np.argmax(counts))
    return pd.Timestamp(starts_arr[best_idx])


def capture_heavy_window(df, heavy_start, window_hours):
    window = pd.Timedelta(hours=window_hours)
    heavy = df[(df["ts"] >= heavy_start) & (df["ts"] < heavy_start + window)].copy()
    if heavy.empty:
        raise SystemExit(f"No events found in heavy window starting {heavy_start}")
    heavy["offset_s"] = (heavy["ts"] - heavy_start).dt.total_seconds()
    return heavy.sort_values("offset_s").reset_index(drop=True)


def build_volatile(heavy, window_hours, repeats, burst_speedup, gap_wall_min):
    burst_wall_len = (window_hours * 3600) / burst_speedup
    gap_wall_len = gap_wall_min * 60

    wall_cursor = 0.0
    blocks = []
    for i in range(repeats):
        block = heavy.copy()
        block["emit_offset_s"] = wall_cursor + block["offset_s"] / burst_speedup
        blocks.append(block)
        wall_cursor += burst_wall_len
        if i < repeats - 1:
            wall_cursor += gap_wall_len

    volatile = pd.concat(blocks, ignore_index=True)[["taxi_id", "emit_offset_s", "lon", "lat"]]

    diffs = np.diff(volatile["emit_offset_s"].values)
    assert np.all(diffs >= 0), "emit_offset_s is not non-decreasing across blocks"

    return volatile, burst_wall_len, wall_cursor


def build_steady(volatile, total_wall_s):
    steady = volatile.copy()
    steady["emit_offset_s"] = np.linspace(0, total_wall_s, len(steady))
    return steady


def per_second_rate(emit_offset_s):
    """Events per 1-second bin, counting only seconds with >=1 event. Gap seconds contribute
    no rows at all (not zero-count rows), so they're naturally excluded from this series -
    every bucket here is a real, active (burst) second."""
    buckets = np.floor(emit_offset_s).astype(int)
    return pd.Series(buckets).value_counts()


def plot_rate_overlay(volatile, steady, total_wall_s, out_path):
    full_index = np.arange(0, int(np.ceil(total_wall_s)) + 2)

    volatile_series = pd.Series(np.floor(volatile["emit_offset_s"]).astype(int)).value_counts().reindex(
        full_index, fill_value=0
    )
    steady_series = pd.Series(np.floor(steady["emit_offset_s"]).astype(int)).value_counts().reindex(
        full_index, fill_value=0
    )

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.bar(full_index, volatile_series.values, width=1.0, label="volatile", alpha=0.7)
    ax.plot(full_index, steady_series.values, color="black", linewidth=1.5, label="steady")
    ax.set_xlabel("Wall-clock seconds from replay start")
    ax.set_ylabel("Events per second")
    ax.set_title("T-Drive workload: volatile bursts vs. steady constant rate")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_csv", nargs="?", default=None)
    parser.add_argument("--window-hours", type=float, default=2.0)
    parser.add_argument("--repeats", type=int, default=4)
    parser.add_argument("--burst-speedup", type=float, default=60.0)
    parser.add_argument("--gap-wall-min", type=float, default=20.0)
    parser.add_argument("--idle-scaledown-min", type=float, default=15.0)
    parser.add_argument("--heavy-start", type=str, default="")
    parser.add_argument("--out-dir", type=str, default=None)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    in_path = Path(args.input_csv) if args.input_csv else repo_root / "data" / "tdrive_moving.csv"
    out_dir = Path(args.out_dir) if args.out_dir else repo_root / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path, parse_dates=["ts"]).sort_values("ts").reset_index(drop=True)
    print(f"Loaded {len(df)} moving pings from {in_path} ({df['ts'].min()} to {df['ts'].max()})")

    if args.heavy_start:
        heavy_start = pd.Timestamp(args.heavy_start)
    else:
        heavy_start = find_busiest_window(df, args.window_hours)
        print(f"No --heavy-start given: selected busiest real {args.window_hours}h window, starting {heavy_start}")

    heavy = capture_heavy_window(df, heavy_start, args.window_hours)
    volatile, burst_wall_len, total_wall_s = build_volatile(
        heavy, args.window_hours, args.repeats, args.burst_speedup, args.gap_wall_min
    )
    steady = build_steady(volatile, total_wall_s)

    volatile_path = out_dir / "tdrive_workload_volatile.csv"
    steady_path = out_dir / "tdrive_workload_steady.csv"
    volatile.to_csv(volatile_path, index=False)
    steady.to_csv(steady_path, index=False)

    print(
        f"\nHeavy window: {heavy_start} to {heavy_start + pd.Timedelta(hours=args.window_hours)} "
        f"({len(heavy)} real events, {heavy['taxi_id'].nunique()} distinct taxis)"
    )
    print(
        f"Burst: {args.window_hours}h of real time compressed {args.burst_speedup}x -> "
        f"{burst_wall_len:.1f}s wall-clock per burst"
    )
    print(f"Gap: {args.gap_wall_min} wall-clock minutes, uncompressed, between bursts")
    print(
        f"Repeats: {args.repeats} -> total wall duration {total_wall_s:.1f}s "
        f"({total_wall_s / 60:.1f} min, {args.repeats - 1} gaps)"
    )

    assert len(volatile) == len(steady), "volatile/steady event count mismatch"
    print(f"\nVolatile workload: {len(volatile)} events -> {volatile_path}")
    print(f"Steady workload:   {len(steady)} events -> {steady_path} (same events, same total duration)")

    print("\n--- Cloud Run idle-scaledown sanity check ---")
    print(f"Gap: {args.gap_wall_min} wall-clock min (direct, no speedup applied)")
    margin = args.gap_wall_min - args.idle_scaledown_min
    if args.gap_wall_min > args.idle_scaledown_min:
        print(
            f"OK: gap ({args.gap_wall_min} min) exceeds the idle-scaledown window "
            f"({args.idle_scaledown_min} min) by {margin:.1f} min - the service should scale to "
            f"zero between bursts, so each burst genuinely exercises a cold start."
        )
    else:
        print(
            f"WARNING: gap ({args.gap_wall_min} min) does not exceed the idle-scaledown window "
            f"({args.idle_scaledown_min} min) - instances may not fully spin down between bursts."
        )

    active_rate = per_second_rate(volatile["emit_offset_s"].values)
    peak_rate = active_rate.max()
    median_rate = active_rate.median()
    steady_rate = len(steady) / total_wall_s

    print("\n--- Peak vs. steady throughput ---")
    print(f"Volatile: peak {peak_rate:.0f} events/sec, median (within-burst) {median_rate:.0f} events/sec")
    print(f"Steady:   constant {steady_rate:.1f} events/sec")
    print(f"Load ratio (volatile peak / steady constant): {peak_rate / steady_rate:.1f}x")
    if peak_rate < PEAK_RATE_FLOOR:
        print(
            f"NOTE: peak rate ({peak_rate:.0f}/sec) is under {PEAK_RATE_FLOOR}/sec - may not be "
            f"enough to pressure a single instance. Increase --burst-speedup to sharpen it."
        )

    rate_png_path = out_dir / "tdrive_workload_rate.png"
    plot_rate_overlay(volatile, steady, total_wall_s, rate_png_path)
    print(f"\nSaved {rate_png_path}")

    print("\n--- What this configuration tests ---")
    print(
        "Scale-to-ZERO cold starts (harshest case): --gap-wall-min comfortably above "
        "--idle-scaledown-min, run with Cloud Run min-instances=0."
    )
    print(
        "Scale-UP lag only (warm but insufficient pool): a shorter gap is fine here - you're "
        "testing how fast instances are added under a burst, not recovery from zero."
    )
    if args.gap_wall_min > args.idle_scaledown_min:
        print(
            f"Current settings (gap={args.gap_wall_min} min > idle-scaledown={args.idle_scaledown_min} min) "
            f"support SCALE-TO-ZERO cold-start testing."
        )
    else:
        print(
            f"Current settings (gap={args.gap_wall_min} min <= idle-scaledown={args.idle_scaledown_min} min) "
            f"only support SCALE-UP lag testing - not a reliable cold-start test."
        )


if __name__ == "__main__":
    main()
