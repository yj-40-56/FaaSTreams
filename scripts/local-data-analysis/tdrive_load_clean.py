#!/usr/bin/env python3
"""Load, clean, and merge the T-Drive per-taxi GPS files into one time-sorted stream.

Usage:
    python tdrive_load_clean.py [input_dir] [output.csv]

input_dir defaults to data/release/taxi_log_2008_by_id/
output.csv defaults to data/tdrive_clean.csv

Each raw line is: taxi_id, datetime, longitude, latitude (no header, lon before lat).
Cleaning per file (done before concatenating, to keep peak memory down): parse timestamps
(dropping unparseable rows), drop exact (taxi_id, ts) duplicates, keep only points inside
the Beijing bounding box, and drop (0, 0) null-island points.
"""
import sys
import time
from pathlib import Path

import pandas as pd

LAT_MIN, LAT_MAX = 39.4, 41.1
LON_MIN, LON_MAX = 115.7, 117.4


def load_and_clean_file(path):
    df = pd.read_csv(
        path,
        header=None,
        names=["taxi_id", "ts", "lon", "lat"],
        skipinitialspace=True,
        on_bad_lines="skip",
    )
    raw = len(df)

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(subset=["taxi_id", "ts"])

    in_box = df["lat"].between(LAT_MIN, LAT_MAX) & df["lon"].between(LON_MIN, LON_MAX)
    df = df[in_box]

    null_island = (df["lon"] == 0) & (df["lat"] == 0)
    df = df[~null_island]

    return df, raw


def main():
    default_in = Path(__file__).resolve().parents[2] / "data" / "release" / "taxi_log_2008_by_id"
    default_out = Path(__file__).resolve().parents[2] / "data" / "tdrive_clean.csv"

    in_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else default_in
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_out

    files = sorted(in_dir.glob("*.txt"))
    print(f"Found {len(files)} taxi files in {in_dir}")

    print("Sample lines (schema check):")
    for f in files[:2]:
        with open(f) as fh:
            for _ in range(3):
                line = fh.readline()
                if not line:
                    break
                print(f"  [{f.name}] {line.rstrip()}")

    raw_total = 0
    cleaned_frames = []
    start = time.time()
    for i, f in enumerate(files, 1):
        cleaned, raw = load_and_clean_file(f)
        raw_total += raw
        if len(cleaned):
            cleaned_frames.append(cleaned)
        if i % 1000 == 0:
            print(f"  ...processed {i}/{len(files)} files ({time.time() - start:.0f}s elapsed)")

    combined = pd.concat(cleaned_frames, ignore_index=True)
    combined = combined.sort_values("ts", kind="mergesort").reset_index(drop=True)

    surviving = len(combined)
    print(f"\nRaw points (pre-clean, summed across files): {raw_total}")
    print(f"Surviving points (parsed, deduped, in-box, no null-island): {surviving}")
    print(f"Dropped: {raw_total - surviving} ({(raw_total - surviving) / raw_total:.1%})")
    print(f"Time range: {combined['ts'].min()} to {combined['ts'].max()}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)
    print(f"Wrote {surviving} rows to {out_path}")


if __name__ == "__main__":
    main()
