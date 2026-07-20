#!/usr/bin/env python3
"""Classify T-Drive points as moving or stationary based on displacement from the previous ping.

Usage:
    python tdrive_movement.py [input.csv] [output.csv]

input.csv defaults to data/tdrive_clean.csv
output.csv defaults to data/tdrive_moving.csv

Per taxi_id, sorted by time, computes the haversine distance to the previous ping. A point
is "stationary" if that displacement is under STATIONARY_THRESHOLD_M (parked/idle GPS drift,
e.g. a taxi pinging the same spot all night). Only moving points are written to output.csv.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

STATIONARY_THRESHOLD_M = 40.0
EARTH_RADIUS_M = 6371000.0


def haversine_m(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, (lon1, lat1, lon2, lat2))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


def main():
    default_in = Path(__file__).resolve().parents[2] / "data" / "tdrive_clean.csv"
    default_out = Path(__file__).resolve().parents[2] / "data" / "tdrive_moving.csv"

    in_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_in
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_out

    df = pd.read_csv(in_path, parse_dates=["ts"])
    df = df.sort_values(["taxi_id", "ts"]).reset_index(drop=True)

    prev_lon = df.groupby("taxi_id")["lon"].shift(1)
    prev_lat = df.groupby("taxi_id")["lat"].shift(1)

    dist = haversine_m(prev_lon.values, prev_lat.values, df["lon"].values, df["lat"].values)
    # First point per taxi has no previous ping to compare against; treat it as moving.
    df["moving"] = np.where(np.isnan(dist), True, dist >= STATIONARY_THRESHOLD_M)

    stationary_frac = 1 - df["moving"].mean()
    print(f"Total pings: {len(df)}")
    print(f"Stationary (displacement < {STATIONARY_THRESHOLD_M:.0f}m from previous ping): {stationary_frac:.1%}")
    print(f"Moving: {1 - stationary_frac:.1%}")

    moving = (
        df.loc[df["moving"], ["taxi_id", "ts", "lon", "lat"]]
        .sort_values("ts")
        .reset_index(drop=True)
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    moving.to_csv(out_path, index=False)
    print(f"Wrote {len(moving)} moving rows to {out_path}")


if __name__ == "__main__":
    main()
