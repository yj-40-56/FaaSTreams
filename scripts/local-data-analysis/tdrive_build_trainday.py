#!/usr/bin/env python3
"""Attach real T-Drive taxi rows to the exact 96-minute train-day schedule.

The schedule is imported from train_day.py so the synthetic CPU benchmark and the
end-to-end data benchmark cannot silently diverge. Taxi timestamps are intentionally
not copied: the replay driver assigns wall-clock RFC3339 timestamps when publishing,
which is required for the live windower to close the correct windows.
"""

import argparse
import csv
import sys
from pathlib import Path


def main():
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root))
    import train_day

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=str(repo_root / "data" / "tdrive_moving.csv"),
        help="cleaned, time-sorted T-Drive moving-ping CSV",
    )
    parser.add_argument(
        "--output",
        default=str(repo_root / "data" / "tdrive_workload_trainday.csv"),
        help="output replay workload",
    )
    args = parser.parse_args()

    offsets = train_day.build_schedule()
    expected_duration = train_day.RUN_WALL_SECONDS
    expected_active = train_day.ACTIVE_WALL_SECONDS
    if expected_duration != 5760 or expected_active != 4560:
        raise SystemExit(
            "Refusing to build: train_day defaults no longer describe a 96-minute run "
            "with 76 active minutes and a 20-minute final shutdown"
        )

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with input_path.open(newline="", encoding="utf-8") as source, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as destination:
        reader = csv.DictReader(source)
        required = {"taxi_id", "ts", "lon", "lat"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"Input columns must include {sorted(required)}; got {reader.fieldnames}")

        writer = csv.DictWriter(
            destination, fieldnames=["taxi_id", "emit_offset_s", "lon", "lat"]
        )
        writer.writeheader()
        for offset, row in zip(offsets, reader):
            if not row["taxi_id"] or not row["lon"] or not row["lat"]:
                raise SystemExit(f"Invalid taxi record after {written} rows: {row}")
            float(row["lon"])
            float(row["lat"])
            writer.writerow(
                {
                    "taxi_id": row["taxi_id"],
                    "emit_offset_s": f"{offset:.9f}",
                    "lon": row["lon"],
                    "lat": row["lat"],
                }
            )
            written += 1

    if written != len(offsets):
        output_path.unlink(missing_ok=True)
        raise SystemExit(f"Input ended after {written} rows; schedule needs {len(offsets)}")

    print(f"Wrote {written} real T-Drive events to {output_path}")
    print("Schedule: 96.0 wall minutes at 15x compression")
    print("Active: simulated 06:00-01:00 = 76.0 wall minutes")
    print("Shutdown: simulated 01:00-06:00 = 20.0 wall minutes with zero events")
    print(f"Peak rate: {train_day.PEAK_RATE:g} events/s")


if __name__ == "__main__":
    main()
