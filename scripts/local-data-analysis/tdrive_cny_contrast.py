#!/usr/bin/env python3
"""Compare moving-taxi activity on Chinese New Year (6-7 Feb 2008) vs baseline weekdays
(4-5 Feb 2008), by hour of day.

Usage:
    python tdrive_cny_contrast.py [moving_only.csv]

moving_only.csv defaults to data/tdrive_moving.csv

The T-Drive week straddles Spring Festival Eve (6 Feb 2008) and Chinese New Year's Day
(7 Feb 2008). Prints hourly counts for both groups side by side and saves an hour-of-day
overlay PNG.
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CNY_DAYS = [pd.Timestamp("2008-02-06").date(), pd.Timestamp("2008-02-07").date()]
BASELINE_DAYS = [pd.Timestamp("2008-02-04").date(), pd.Timestamp("2008-02-05").date()]


def hourly_avg(df, days):
    sub = df[df["ts"].dt.date.isin(days)]
    per_day_hour = sub.groupby([sub["ts"].dt.date, sub["ts"].dt.hour]).size()
    avg_by_hour = per_day_hour.groupby(level=1).mean().reindex(range(24), fill_value=0)
    return avg_by_hour, sub


def main():
    default_in = Path(__file__).resolve().parents[2] / "data" / "tdrive_moving.csv"
    in_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_in

    df = pd.read_csv(in_path, parse_dates=["ts"])

    cny_hourly, cny_sub = hourly_avg(df, CNY_DAYS)
    baseline_hourly, baseline_sub = hourly_avg(df, BASELINE_DAYS)

    print(f"Baseline days: {BASELINE_DAYS} ({len(baseline_sub)} moving pings total)")
    print(f"CNY days:      {CNY_DAYS} ({len(cny_sub)} moving pings total)")
    print()
    print(f"{'hour':>4} {'baseline_avg':>13} {'cny_avg':>10} {'cny/baseline':>13}")
    for h in range(24):
        b = baseline_hourly[h]
        c = cny_hourly[h]
        ratio = c / b if b else float("nan")
        print(f"{h:>4} {b:13.1f} {c:10.1f} {ratio:13.2f}")

    diffs = cny_hourly - baseline_hourly
    biggest_drop_hour = diffs.idxmin()
    biggest_rise_hour = diffs.idxmax()
    print(
        f"\nBiggest CNY drop vs baseline: hour {biggest_drop_hour}:00 "
        f"({baseline_hourly[biggest_drop_hour]:.0f} -> {cny_hourly[biggest_drop_hour]:.0f})"
    )
    print(
        f"Biggest CNY rise vs baseline: hour {biggest_rise_hour}:00 "
        f"({baseline_hourly[biggest_rise_hour]:.0f} -> {cny_hourly[biggest_rise_hour]:.0f})"
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(range(24), baseline_hourly.values, marker="o", label="Baseline (4-5 Feb)")
    ax.plot(range(24), cny_hourly.values, marker="o", label="Chinese New Year (6-7 Feb)")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Avg moving-taxi pings per hour")
    ax.set_title("T-Drive moving-only pings by hour: CNY vs baseline weekdays")
    ax.set_xticks(range(0, 24, 2))
    ax.legend()
    fig.tight_layout()
    out_png = Path(__file__).resolve().parents[2] / "data" / "tdrive_cny_hourly.png"
    fig.savefig(out_png, dpi=150)
    print(f"\nSaved {out_png}")


if __name__ == "__main__":
    main()
