#!/usr/bin/env python3
"""Plot number of AIS events per minute from a CSV file.

Usage:
    python plot_events_per_minute.py <input.csv> [output.png]
"""
import sys

import matplotlib.pyplot as plt
import pandas as pd


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input.csv> [output.png]")
        sys.exit(1)

    csv_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else "events_per_minute.png"

    # Timestamp is always the first column; read only that one to save memory/time.
    timestamps = pd.read_csv(csv_path, usecols=[0], header=0, dtype=str).iloc[:, 0]
    ts = pd.to_datetime(timestamps, format="%d/%m/%Y %H:%M:%S")
    minute_counts = ts.dt.floor("min").value_counts().sort_index()

    minutes = minute_counts.index
    values = minute_counts.values
    labels = minutes.strftime("%H:%M")

    fig, ax = plt.subplots(figsize=(min(40, max(10, len(minutes) * 0.15)), 6))
    ax.bar(range(len(minutes)), values, width=1.0)
    ax.set_xlabel("Minute")
    ax.set_ylabel("Number of events")
    ax.set_title(f"Events per minute -- {csv_path}")

    step = max(1, len(minutes) // 40)
    ax.set_xticks(range(0, len(minutes), step))
    ax.set_xticklabels(labels[::step], rotation=90)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved plot to {out_path} ({len(minutes)} minutes, {int(values.sum())} events)")


if __name__ == "__main__":
    main()
