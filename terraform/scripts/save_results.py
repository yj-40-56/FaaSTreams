#!/usr/bin/env python3
"""
Reads the most recent benchmark window results from Cloud Logging and saves
them to results/{env}_{window_size}s_{timestamp}.json.

Usage:
  python3 scripts/save_results.py --env=baseline --project=faastreams
  python3 scripts/save_results.py --env=baseline --window-size=15 --freshness=10m
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


def read_tfvars(env):
    path = f"environments/{env}.tfvars"
    values = {}
    if not os.path.exists(path):
        return values
    for line in open(path):
        for key in ("env_name", "window_size"):
            m = re.match(rf'{key}\s*=\s*"?([^"\s]+)"?', line)
            if m:
                values[key] = m.group(1)
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="baseline")
    parser.add_argument("--window-size", type=int, help="Override window size label in filename")
    parser.add_argument("--project", default="faastreams")
    parser.add_argument("--freshness", default="15m")
    args = parser.parse_args()

    tfvars = read_tfvars(args.env)
    env_name = tfvars.get("env_name", args.env)
    window_size = args.window_size or int(tfvars.get("window_size", 0)) or None

    log_filter = (
        f'resource.type="cloud_run_revision" AND '
        f'(resource.labels.service_name="coordinator-{env_name}" OR '
        f'resource.labels.service_name="worker-{env_name}") AND '
        f'textPayload!=""'
    )

    print(f"Reading logs for {args.env} (service prefix: {env_name})...")
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
        "env_name": env_name,
        "window_size_seconds": window_size,
        "window": f"{window_start} - {window_end}" if window_start else "unknown",
        "event_count": event_count,
        "logged_at": log_timestamp,
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "queries": queries,
    }

    os.makedirs("results", exist_ok=True)
    ts_label = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    w_label = f"{window_size}s" if window_size else "unknown"
    filename = f"results/{args.env}_{w_label}_{ts_label}.json"

    with open(filename, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Saved → {filename}")
    print(f"  Window : {output['window']}")
    print(f"  Events : {event_count}")
    for name, rows in queries.items():
        print(f"  {name}: {len(rows)} row(s)")


if __name__ == "__main__":
    main()
