#!/usr/bin/env python3
"""
Reads the most recent benchmark window results from Cloud Logging (windower +
worker) and saves them to results/{env}_{timestamp}.json.

Usage:
  python3 scripts/save_results.py --env=live --project=faastreams
  python3 scripts/save_results.py --env=live --freshness=10m
"""
import argparse
import ast
import json
import os
import re
import subprocess
import sys
from datetime import datetime

RESULT_RE = re.compile(r'\[Worker:(\w+)\] \d+ result\(s\): (.+)$')
WINDOW_RE = re.compile(r'Received window (\S+) - (\S+) \(\d+-\d+\)')
FETCH_RE = re.compile(r'\[Fetch\] Found (\d+) member\(s\)')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="live", help="'live' = unsuffixed service names; anything else reads '<service>-<env>'")
    parser.add_argument("--project", default="faastreams")
    parser.add_argument("--freshness", default="15m")
    args = parser.parse_args()

    suffix = "" if args.env == "live" else f"-{args.env}"
    windower_name = f"windower{suffix}"
    worker_name = f"worker{suffix}"

    log_filter = (
        f'resource.type="cloud_run_revision" AND '
        f'(resource.labels.service_name="{windower_name}" OR '
        f'resource.labels.service_name="{worker_name}") AND '
        f'textPayload!=""'
    )

    print(f"Reading logs for env={args.env} ({windower_name}, {worker_name})...")
    result = subprocess.run(
        ["gcloud", "logging", "read", log_filter,
         "--project", args.project, "--limit", "500",
         "--format", "json", "--freshness", args.freshness],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    entries = json.loads(result.stdout)

    queries = {}
    window_start = window_end = None
    event_count = None
    log_timestamp = None

    for entry in entries:
        payload = entry.get("textPayload", "")
        ts = entry.get("timestamp", "")

        m = RESULT_RE.search(payload)
        if m:
            name, raw = m.group(1), m.group(2)
            if name not in queries:
                try:
                    queries[name] = ast.literal_eval(raw)
                except Exception:
                    queries[name] = raw
                if log_timestamp is None:
                    log_timestamp = ts

        m = WINDOW_RE.search(payload)
        if m and window_start is None:
            window_start, window_end = m.group(1), m.group(2)

        m = FETCH_RE.search(payload)
        if m and event_count is None:
            event_count = int(m.group(1))

    if not queries:
        print("No results found. Run the simulator first, then retry.")
        sys.exit(1)

    output = {
        "env": args.env,
        "window": f"{window_start} - {window_end}" if window_start else "unknown",
        "event_count": event_count,
        "logged_at": log_timestamp,
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "queries": queries,
    }

    os.makedirs("results", exist_ok=True)
    ts_label = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"results/{args.env}_{ts_label}.json"

    with open(filename, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Saved -> {filename}")
    print(f"  Window : {output['window']}")
    print(f"  Events : {event_count}")
    for name, rows in queries.items():
        print(f"  {name}: {len(rows)} row(s)")


if __name__ == "__main__":
    main()
