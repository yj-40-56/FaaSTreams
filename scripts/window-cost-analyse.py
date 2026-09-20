#!/usr/bin/env python3
"""Fits the window-size cost model from a window-cost-series.sh run.

    scripts/window-cost-analyse.py window-cost-<ts>/manifest.json

Takes the log time ranges from the manifest rather than hand-written ones: the
first version of this analysis used shell string-slicing to build them, produced
malformed ranges, and returned empty results that were read as evidence of
absence.

Two rules do the real work:

* Lines are grouped by `labels.execution_id`, one per worker invocation. Pairing
  by timestamp order misattributes lines whenever worker instances run windows
  concurrently, which they do at every window size.
* A window is steady-state by its own properties -- close latency near the run's
  floor, event count near the modal -- not by its position in the run. A
  positional "drop the first and last" rule keeps stall-hatch and
  drain-damaged windows whenever they are not literally last.
"""
import json, math, re, statistics, subprocess, sys
from datetime import datetime
from pathlib import Path

RECV = re.compile(r"Received window .* \((\d+)-(\d+)\)")
LOADED = re.compile(r"Loaded (\d+) records")

LATENCY_FACTOR = 2.0    # drop a window closing at more than this times the run's floor
COUNT_TOLERANCE = 0.05  # drop a window whose count is further than this from the modal
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 10: 2.228, 20: 2.086}


def t_crit(df):
    for k in sorted(T95):
        if df <= k:
            return T95[k]
    return 1.96


def fetch(project, service, frm, to, cache):
    if cache.exists():
        return json.loads(cache.read_text())
    out = subprocess.run(
        ["gcloud", "logging", "read",
         f'resource.labels.service_name="{service}" AND timestamp>="{frm}" AND timestamp<"{to}"',
         "--project", project, "--limit", "5000", "--format", "json"],
        capture_output=True, text=True, check=True).stdout
    cache.write_text(out)
    return json.loads(out)


def windows(entries):
    def ts(e):
        return datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")).timestamp()
    by_exec = {}
    for e in entries:
        text = e.get("textPayload") or ""
        ex = (e.get("labels") or {}).get("execution_id")
        if not ex:
            continue
        r = by_exec.setdefault(ex, {})
        if m := RECV.search(text):
            r.update(start=int(m.group(1)), end=int(m.group(2)), recv=ts(e))
        elif m := LOADED.search(text):
            r["events"] = int(m.group(1))
        elif text.strip().endswith("Done."):
            r["done"] = ts(e)
    rows = []
    for r in by_exec.values():
        if {"start", "end", "recv", "events", "done"} <= r.keys():
            r["lat"] = r["recv"] - r["end"]
            r["dur"] = r["done"] - r["recv"]
            rows.append(r)
    return sorted(rows, key=lambda r: r["start"])


def steady(rows):
    """Latency first: it marks healthy windows independent of size. The modal
    count is then taken over those, because a median over all windows is dragged
    by the damaged ones it is meant to exclude."""
    if not rows:
        return [], []
    floor = min(r["lat"] for r in rows)
    healthy = [r for r in rows if r["lat"] <= LATENCY_FACTOR * floor]
    modal = statistics.median(r["events"] for r in healthy) if healthy else 0
    keep, drop = [], []
    for r in rows:
        if r["lat"] > LATENCY_FACTOR * floor:
            drop.append((r, f"lat {r['lat']:.0f}s > {LATENCY_FACTOR:g}x floor {floor:.0f}s"))
        elif modal and abs(r["events"] - modal) / modal > COUNT_TOLERANCE:
            drop.append((r, f"events {r['events']:,} vs modal {modal:,.0f}"))
        else:
            keep.append(r)
    return keep, drop


def fit(pairs):
    n = len(pairs)
    if n < 3:
        return None
    xs, ys = [p[0] for p in pairs], [p[1] for p in pairs]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in pairs) / sxx
    inter = my - slope * mx
    ss_res = sum((y - (inter + slope * x)) ** 2 for x, y in pairs)
    ss_tot = sum((y - my) ** 2 for y in ys)
    s2 = ss_res / (n - 2)
    t = t_crit(n - 2)
    se_s = math.sqrt(s2 / sxx)
    se_i = math.sqrt(s2 * (1 / n + mx * mx / sxx))
    return dict(n=n, fixed=inter, var=slope, r2=1 - ss_res / ss_tot,
                fixed_ci=(inter - t * se_i, inter + t * se_i),
                var_ci=(slope - t * se_s, slope + t * se_s))


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    mpath = Path(sys.argv[1])
    m = json.loads(mpath.read_text())
    cache_dir = mpath.parent
    project = m["project"]

    pooled, table = [], []
    for run in m["runs"]:
        size = run["window"]
        entries = fetch(project, "worker", run["from"], run["to"],
                        cache_dir / f"worker-{size}s.json")
        rows = windows(entries)
        keep, drop = steady(rows)
        total = sum(r["events"] for r in rows)

        print(f"\n=== {size}s window : {len(rows)} windows, {len(keep)} steady ===")
        for r, why in drop:
            print(f"    dropped [{r['start']}] ev={r['events']:>8,} lat={r['lat']:>7.1f}s  {why}")
        gap = run["published"] - total
        flag = "" if gap == 0 else f"  <-- {gap:,} unaccounted ({gap/run['published']:.1%})"
        print(f"    published {run['published']:,} / in windows {total:,}{flag}")
        if run.get("leftover"):
            print(f"    {run['leftover']:,} events left in Redis: they reached no window")

        if keep:
            ev = statistics.mean(r["events"] for r in keep)
            dur = statistics.mean(r["dur"] for r in keep)
            lats = sorted(r["lat"] for r in keep)
            table.append((size, len(keep), ev, dur, statistics.median(lats), max(lats)))
            pooled += [(r["events"], r["dur"]) for r in keep]

    print("\n" + "=" * 86)
    print(f"{'window':>7} {'n':>4} {'events/win':>12} {'ratio':>7} {'worker_s':>9} "
          f"{'us/event':>9} {'lat med':>9} {'lat max':>9}")
    base = table[0][2] if table else 1
    for size, n, ev, dur, lmed, lmax in table:
        print(f"{size:>6}s {n:>4} {ev:>12,.0f} {ev/base:>7.3f} {dur:>9.3f} "
              f"{dur/ev*1e6:>9.2f} {lmed:>8.1f}s {lmax:>8.1f}s")
        if n < 5:
            print(f"        !! only {n} steady windows -- raise WINDOWS_TARGET for this size")

    if table:
        print(f"\nworker-seconds per 60s of data:")
        b = 60 / table[0][0] * table[0][3]
        for size, _, _, dur, _, _ in table:
            tot = 60 / size * dur
            print(f"  {size:>3}s: {60/size:>4.1f} invocations x {dur:.3f}s = {tot:>6.2f}s  ({tot/b:.2f}x)")

    f = fit(pooled)
    if not f:
        print("\nnot enough clean windows to fit")
        return
    print("\n" + "=" * 86)
    print(f"FIT over {f['n']} steady windows (per-window observations, not group means)")
    print(f"  fixed    = {f['fixed']:.3f} s/invocation   95% CI [{f['fixed_ci'][0]:.3f}, {f['fixed_ci'][1]:.3f}]")
    print(f"  variable = {f['var']*1e6:.2f} us/event        95% CI [{f['var_ci'][0]*1e6:.2f}, {f['var_ci'][1]*1e6:.2f}]")
    print(f"  R^2      = {f['r2']:.4f}")
    be = f["fixed"] / f["var"]
    lo = f["fixed_ci"][0] / f["var_ci"][1]
    hi = f["fixed_ci"][1] / f["var_ci"][0]
    print(f"\n  fixed == variable at {be:,.0f} events/window   (CI {lo:,.0f} - {hi:,.0f})")
    if table:
        rate = table[0][2] / table[0][0]
        print(f"  at the measured {rate:,.0f} ev/s that is a {be/rate:.0f}s window "
              f"(range {lo/rate:.0f}-{hi/rate:.0f}s)")
    print("\n  Quote the range, not the point: it is a ratio of two fitted parameters,\n"
          "  and a 50% crossover is an arbitrary threshold, not an economic one.\n"
          "  worker_s excludes container start, so the short-window penalty is a lower bound.")


if __name__ == "__main__":
    main()
