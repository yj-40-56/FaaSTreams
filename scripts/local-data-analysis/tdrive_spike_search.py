#!/usr/bin/env python3
"""Hunt for arrival-rate spikes in the T-Drive stream: bursts in events per time window,
concentrated in space, driven by moving taxis (not idle ones).

Usage:
    python tdrive_spike_search.py [all_pings.csv] [moving_only.csv]

all_pings.csv defaults to data/tdrive_clean.csv
moving_only.csv defaults to data/tdrive_moving.csv

Runs the same global + spatial spike analysis on both streams and compares them, to separate
real bursts from parked-taxi GPS drift. Writes data/tdrive_spike_timeseries.png (moving-only
5-min series for the full week, with the top spikes marked).
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

GRID_DEG = 0.01
GLOBAL_WINDOWS = ["1min", "5min", "15min"]
SPATIAL_WINDOW = "5min"
MIN_NONEMPTY_WINDOWS = 20  # min active 5-min windows for a cell to be ranked (avoid noise/div-by-near-zero)
TOP_N = 20
SUSPECT_TAXI_THRESHOLD = 5  # flag spikes driven by fewer than this many distinct taxis
SUSPECT_PINGS_PER_TAXI = 3  # flag spikes where a handful of taxis account for most events (GPS jitter,
# not real convergence) - overall avg ping interval is ~177s, so a 5-min window "should" see ~1.7
# pings/taxi; a single taxi bouncing back and forth on noisy GPS can rack up 10+ in the same window
DURATION_CONTEXT = 3  # windows of context on each side for the top-5 duration printout


def load_stream(path):
    df = pd.read_csv(path, parse_dates=["ts"])
    return df.sort_values("ts").reset_index(drop=True)


def global_window_stats(df, window):
    counts = df.set_index("ts").resample(window).size()
    median = counts.median()
    p99 = counts.quantile(0.99)
    peak = counts.max()
    return {
        "window": window,
        "median": median,
        "p99": p99,
        "max": peak,
        "peak_to_median": peak / median if median else float("nan"),
        "p99_to_median": p99 / median if median else float("nan"),
    }


def print_global_table(name, df):
    print(f"\n--- Global window stats: {name} ---")
    print(f"{'window':>8} {'median':>8} {'p99':>8} {'max':>8} {'peak/med':>10} {'p99/med':>10}")
    for w in GLOBAL_WINDOWS:
        s = global_window_stats(df, w)
        print(
            f"{s['window']:>8} {s['median']:8.1f} {s['p99']:8.1f} {s['max']:8.0f} "
            f"{s['peak_to_median']:10.2f} {s['p99_to_median']:10.2f}"
        )


def assign_grid(df):
    cell_lon = (np.floor(df["lon"] / GRID_DEG) * GRID_DEG).round(4)
    cell_lat = (np.floor(df["lat"] / GRID_DEG) * GRID_DEG).round(4)
    df = df.assign(
        cell_lon_center=(cell_lon + GRID_DEG / 2).round(4),
        cell_lat_center=(cell_lat + GRID_DEG / 2).round(4),
    )
    df["cell"] = cell_lon.astype(str) + "," + cell_lat.astype(str)
    df["window_start"] = df["ts"].dt.floor(SPATIAL_WINDOW)
    return df


def spatial_spikes(grid_df):
    grouped = (
        grid_df.groupby(["cell", "cell_lon_center", "cell_lat_center", "window_start"])
        .agg(count=("taxi_id", "size"), distinct_taxis=("taxi_id", "nunique"))
        .reset_index()
    )

    cell_stats = grouped.groupby("cell")["count"].agg(nonempty_windows="size", cell_median="median")
    grouped = grouped.merge(cell_stats, on="cell")
    grouped = grouped[grouped["nonempty_windows"] >= MIN_NONEMPTY_WINDOWS].copy()
    grouped["ratio"] = grouped["count"] / grouped["cell_median"]
    grouped["pings_per_taxi"] = grouped["count"] / grouped["distinct_taxis"]
    grouped["suspect"] = (grouped["distinct_taxis"] < SUSPECT_TAXI_THRESHOLD) | (
        grouped["pings_per_taxi"] > SUSPECT_PINGS_PER_TAXI
    )

    return grouped.sort_values("ratio", ascending=False).reset_index(drop=True)


def print_top_spikes(name, ranked):
    print(f"\n--- Top {TOP_N} spatial spikes (5-min grid, cells with >={MIN_NONEMPTY_WINDOWS} active windows): {name} ---")
    top = ranked.head(TOP_N)
    print(
        f"{'cell':>16} {'lon':>9} {'lat':>8} {'window_start':>20} {'count':>6} "
        f"{'cell_med':>9} {'ratio':>7} {'taxis':>6} {'pings/taxi':>10}"
    )
    for _, r in top.iterrows():
        suspect = " (suspect: noise, not demand)" if r["suspect"] else ""
        print(
            f"{r['cell']:>16} {r['cell_lon_center']:9.4f} {r['cell_lat_center']:8.4f} "
            f"{str(r['window_start']):>20} {int(r['count']):6d} {r['cell_median']:9.1f} "
            f"{r['ratio']:7.2f} {int(r['distinct_taxis']):6d} {r['pings_per_taxi']:10.2f}{suspect}"
        )
    return top


def print_duration_context(name, grid_df, top):
    print(f"\n--- Duration context (top 5 spikes): {name} ---")
    for _, spike in top.head(5).iterrows():
        cell = spike["cell"]
        center = spike["window_start"]
        window_range = pd.date_range(
            center - DURATION_CONTEXT * pd.Timedelta(SPATIAL_WINDOW),
            center + DURATION_CONTEXT * pd.Timedelta(SPATIAL_WINDOW),
            freq=SPATIAL_WINDOW,
        )
        cell_points = grid_df[grid_df["cell"] == cell]
        counts = cell_points.groupby("window_start").size().reindex(window_range, fill_value=0)
        print(f"\nCell {cell} around {center}:")
        for ts, c in counts.items():
            marker = "  <-- spike" if ts == center else ""
            print(f"  {ts}  {c:>4}{marker}")


def main():
    default_all = Path(__file__).resolve().parents[2] / "data" / "tdrive_clean.csv"
    default_moving = Path(__file__).resolve().parents[2] / "data" / "tdrive_moving.csv"

    all_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_all
    moving_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_moving

    streams = {
        "all pings": load_stream(all_path),
        "moving-only": load_stream(moving_path),
    }

    ranked_by_stream = {}
    top_by_stream = {}
    for name, df in streams.items():
        print_global_table(name, df)
        grid_df = assign_grid(df)
        ranked = spatial_spikes(grid_df)
        ranked_by_stream[name] = ranked
        top = print_top_spikes(name, ranked)
        top_by_stream[name] = top
        print_duration_context(name, grid_df, top)

    print("\n--- All pings vs moving-only: how much do spikes shrink when idle pings are removed? ---")
    all_top_ratio = ranked_by_stream["all pings"]["ratio"].max()
    moving_top_ratio = ranked_by_stream["moving-only"]["ratio"].max()
    shrink = (all_top_ratio - moving_top_ratio) / all_top_ratio if all_top_ratio else float("nan")
    print(f"Top peak/median ratio, all pings:   {all_top_ratio:.2f}")
    print(f"Top peak/median ratio, moving-only: {moving_top_ratio:.2f}")
    print(f"Shrinkage removing idle pings: {shrink:.1%}")

    moving_df = streams["moving-only"]
    counts = moving_df.set_index("ts").resample(SPATIAL_WINDOW).size()
    fig, ax = plt.subplots(figsize=(20, 6))
    ax.bar(counts.index, counts.values, width=pd.Timedelta(SPATIAL_WINDOW), align="edge")
    for t in top_by_stream["moving-only"]["window_start"]:
        ax.axvline(t, color="red", alpha=0.5, linewidth=1)
    ax.set_xlabel("Time")
    ax.set_ylabel("Events per 5 min (moving-only)")
    ax.set_title("T-Drive moving-only events per 5-min window, top spatial spikes marked in red")
    fig.autofmt_xdate()
    fig.tight_layout()
    out_png = Path(__file__).resolve().parents[2] / "data" / "tdrive_spike_timeseries.png"
    fig.savefig(out_png, dpi=150)
    print(f"\nSaved {out_png}")

    moving_top = top_by_stream["moving-only"]
    non_suspect = moving_top[~moving_top["suspect"]]
    print("\n=== VERDICT ===")
    if len(non_suspect) == 0:
        print(
            "No moving, multi-taxi spike found above the suspect threshold - data looks smooth "
            "once idle pings are removed."
        )
    else:
        best = non_suspect.iloc[0]
        clears = "clears" if best["ratio"] >= 10 else "does NOT clear"
        print(
            f"Sharpest real spike: cell centered at ({best['cell_lon_center']:.4f}, {best['cell_lat_center']:.4f}), "
            f"window starting {best['window_start']}, {int(best['count'])} events vs a cell median of "
            f"{best['cell_median']:.1f} ({best['ratio']:.1f}x), driven by {int(best['distinct_taxis'])} distinct taxis. "
            f"This {clears} the ~10x peak-to-median bar for a genuine high-volatility test case."
        )


if __name__ == "__main__":
    main()
