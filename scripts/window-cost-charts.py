#!/usr/bin/env python3
"""Renders the window-size cost figures from a measurement run.

    scripts/window-cost-charts.py <manifest.json> [out-dir]

Four figures, each answering one question:
  1  where the fixed cost is        -- duration vs window size, intercept shown
  2  what an event costs            -- us/event against window size
  3  what short windows cost you    -- worker-seconds for the same data
  4  why they buy nothing           -- close latency against the window itself
"""
import json, re, statistics, sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SURFACE, INK, INK_2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8985"
S1, S2 = "#2a78d6", "#eb6834"          # categorical slots 1 and 2
GRID = "#e4e3df"

RECV = re.compile(r"Received window .* \((\d+)-(\d+)\)")
LOADED = re.compile(r"Loaded (\d+) records")
LATENCY_FACTOR, COUNT_TOLERANCE = 2.0, 0.05

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK_2, "axes.edgecolor": GRID,
    "xtick.color": INK_2, "ytick.color": INK_2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.axisbelow": True, "figure.dpi": 140,
})


def load(manifest):
    m = json.loads(Path(manifest).read_text())
    cache = Path(manifest).parent
    out = []
    for run in m["runs"]:
        entries = json.loads((cache / f"worker-{run['window']}s.json").read_text())
        by = {}
        for e in entries:
            t = e.get("textPayload") or ""
            ex = (e.get("labels") or {}).get("execution_id")
            if not ex:
                continue
            r = by.setdefault(ex, {})
            if mm := RECV.search(t):
                r.update(start=int(mm.group(1)), end=int(mm.group(2)),
                         recv=datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")).timestamp())
            elif mm := LOADED.search(t):
                r["events"] = int(mm.group(1))
            elif t.strip().endswith("Done."):
                r["done"] = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")).timestamp()
        rows = [r for r in by.values() if {"start", "end", "recv", "events", "done"} <= r.keys()]
        for r in rows:
            r["lat"], r["dur"] = r["recv"] - r["end"], r["done"] - r["recv"]
        floor = min(r["lat"] for r in rows)
        healthy = [r for r in rows if r["lat"] <= LATENCY_FACTOR * floor]
        modal = statistics.median(r["events"] for r in healthy)
        keep = [r for r in healthy if abs(r["events"] - modal) / modal <= COUNT_TOLERANCE]
        out.append(dict(window=run["window"], rows=keep, floor=floor,
                        lat_max=max(r["lat"] for r in keep)))
    return sorted(out, key=lambda d: -d["window"])


def fit(pairs):
    x = np.array([p[0] for p in pairs], float)
    y = np.array([p[1] for p in pairs], float)
    slope, inter = np.polyfit(x, y, 1)
    return inter, slope


def fig1(runs, outdir):
    pts = [(r["events"], r["dur"]) for d in runs for r in d["rows"]]
    inter, slope = fit(pts)
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    xs = np.array([p[0] for p in pts]); ys = np.array([p[1] for p in pts])
    ax.scatter(xs / 1000, ys, s=26, color=S1, alpha=.75, zorder=3,
               edgecolors=SURFACE, linewidths=1.2, label=f"{len(pts)} windows")
    grid = np.linspace(0, xs.max() * 1.05, 100)
    ax.plot(grid / 1000, inter + slope * grid, color=INK_2, lw=2, zorder=4,
            label=f"fit: {inter:.2f}s + {slope*1e6:.2f} µs/event")
    ax.scatter([0], [inter], s=90, color=S2, zorder=5, edgecolors=SURFACE, linewidths=1.5)
    ax.annotate(f"fixed cost {inter:.2f}s per invocation:\nwhat a window costs before it reads an event",
                xy=(0, inter), xytext=(22, 2.75), color=INK,
                fontsize=9.5, fontweight="bold", ha="left",
                arrowprops=dict(arrowstyle="->", color=S2, lw=1.6,
                                connectionstyle="angle3,angleA=0,angleB=80"))
    ax.set_xlim(-6, xs.max() / 1000 * 1.05)
    ax.set_ylim(0, ys.max() * 1.12)
    ax.set_xlabel("events in the window (thousands)")
    ax.set_ylabel("worker seconds")
    ax.set_title("A window costs a fixed price plus a price per event",
                 color=INK, fontsize=12.5, fontweight="bold", loc="left", pad=12)
    ax.legend(frameon=False, loc="lower right", fontsize=9, labelcolor=INK_2)
    save(fig, outdir, "1-cost-model")


def fig2(runs, outdir):
    w = [d["window"] for d in runs]
    per = [statistics.mean(r["dur"] for r in d["rows"]) /
           statistics.mean(r["events"] for r in d["rows"]) * 1e6 for d in runs]
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    xs = np.arange(len(w))
    ax.plot(xs, per, color=S1, lw=2, marker="o", ms=9, zorder=3,
            markeredgecolor=SURFACE, markeredgewidth=1.5)
    for x, y in zip(xs, per):
        ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points",
                    xytext=(0, 12), ha="center", fontsize=9.5,
                    fontweight="bold", color=INK)
    ax.set_xticks(xs); ax.set_xticklabels([f"{v}s" for v in w])
    ax.set_ylim(0, max(per) * 1.2)
    ax.set_xlabel("window size")
    ax.set_ylabel("worker µs per event")
    ax.set_title("Shorter windows cost more per event",
                 color=INK, fontsize=12.5, fontweight="bold", loc="left", pad=12)
    save(fig, outdir, "2-cost-per-event")


def fig3(runs, outdir, breakeven=None):
    w = [d["window"] for d in runs]
    tot = [60 / d["window"] * statistics.mean(r["dur"] for r in d["rows"]) for d in runs]
    base = tot[0]
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    xs = np.arange(len(w))
    ax.bar(xs, tot, width=.62, color=S1, zorder=3)
    for x, t in zip(xs, tot):
        ax.annotate(f"{t/base:.2f}×", (x, t), textcoords="offset points",
                    xytext=(0, 7), ha="center", fontsize=10,
                    fontweight="bold", color=INK)
    ax.set_xticks(xs); ax.set_xticklabels([f"{v}s" for v in w])
    ax.set_ylim(0, max(tot) * 1.18)
    ax.set_xlabel("window size")
    ax.set_ylabel("worker seconds per 60s of data")
    ax.set_title("The same data costs twice as much in 10s windows",
                 color=INK, fontsize=12.5, fontweight="bold", loc="left", pad=12)
    save(fig, outdir, "3-cost-of-short-windows")


def fig4(runs, outdir):
    w = [d["window"] for d in runs]
    med = [statistics.median(r["lat"] for r in d["rows"]) for d in runs]
    lo = [d["floor"] for d in runs]
    hi = [d["lat_max"] for d in runs]
    xs = np.arange(len(w))

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(xs, w, color=S2, lw=2, marker="s", ms=8, zorder=3,
            markeredgecolor=SURFACE, markeredgewidth=1.5, label="the window itself")
    ax.fill_between(xs, lo, hi, color=S1, alpha=.18, zorder=2)
    ax.plot(xs, med, color=S1, lw=2, marker="o", ms=9, zorder=4,
            markeredgecolor=SURFACE, markeredgewidth=1.5,
            label="close latency (median, with range)")

    for x, y in zip(xs, med):
        ax.annotate(f"{y:.1f}s", (x, y), textcoords="offset points",
                    xytext=(0, -18), ha="center", fontsize=9, color=INK_2)

    share = med[-1] / w[-1]
    ax.annotate(f"at {w[-1]}s the wait is {share:.0%} of the window",
                xy=(len(w) - 1.08, (w[-1] + med[-1]) / 2), xytext=(1.6, 31),
                color=INK, fontsize=9.5, fontweight="bold", ha="left",
                arrowprops=dict(arrowstyle="->", color=MUTED, lw=1.4,
                                connectionstyle="angle3,angleA=0,angleB=60"))
    ax.set_xticks(xs); ax.set_xticklabels([f"{v}s" for v in w])
    ax.set_ylim(0, 66)
    ax.set_xlabel("window size")
    ax.set_ylabel("seconds")
    ax.set_title("Latency does not shrink with the window,\nso short windows buy no freshness",
                 color=INK, fontsize=12.5, fontweight="bold", loc="left", pad=12)
    ax.legend(frameon=False, loc="upper right", fontsize=9, labelcolor=INK_2)
    save(fig, outdir, "4-latency-flat")


def save(fig, outdir, name):
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(Path(outdir) / f"{name}.{ext}", bbox_inches="tight")
    plt.close(fig)
    print(f"  {name}.png / .svg")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    outdir = Path(sys.argv[2] if len(sys.argv) > 2 else "charts")
    outdir.mkdir(parents=True, exist_ok=True)
    runs = load(sys.argv[1])
    print(f"figures from {len(runs)} window sizes -> {outdir}/")
    fig1(runs, outdir); fig2(runs, outdir); fig3(runs, outdir); fig4(runs, outdir)


if __name__ == "__main__":
    main()
