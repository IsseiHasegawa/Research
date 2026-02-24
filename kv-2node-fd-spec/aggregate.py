#!/usr/bin/env python3
"""
Aggregate experiment runs into heatmap and scatter CSVs for plotting.
Reads runs from runs/ (each dir has injector.jsonl with detection_latency_ms per run).
Outputs: heatmap.csv (hb_timeout_ms, hb_interval_ms, median_detection_ms, iqr_detection_ms)
         scatter.csv (missed, hb_interval_ms, hb_timeout_ms, median_detection_ms, iqr_detection_ms)
"""
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parent
RUNS_DIR = ROOT / "runs"
OUT_DIR = ROOT / "analysis_out"


def load_latencies_from_injector(run_dir: Path) -> Optional[Tuple[int, int, float]]:
    """Returns (hb_interval_ms, hb_timeout_ms, detection_latency_ms) or None."""
    p = run_dir / "injector.jsonl"
    if not p.exists():
        return None
    with open(p) as f:
        for line in f:
            try:
                obj = json.loads(line)
                if obj.get("event") == "declared_dead":
                    lat = obj.get("detection_latency_ms")
                    if lat is None:
                        continue
                    hb_interval_ms = obj.get("hb_interval_ms")
                    hb_timeout_ms = obj.get("hb_timeout_ms")
                    if hb_interval_ms is None or hb_timeout_ms is None:
                        continue
                    return (int(hb_interval_ms), int(hb_timeout_ms), float(lat))
            except (json.JSONDecodeError, ValueError, KeyError):
                continue
    return None


def collect_runs():
    """Yields (hb_interval_ms, hb_timeout_ms, detection_latency_ms) for each run."""
    if not RUNS_DIR.exists():
        return
    for d in RUNS_DIR.iterdir():
        if not d.is_dir():
            continue
        r = load_latencies_from_injector(d)
        if r is not None and r[2] >= 0:  # skip negative detection_latency_ms (warmup false positive)
            yield r


def main():
    runs = list(collect_runs())
    if not runs:
        print("No runs found in", RUNS_DIR)
        return

    # Group by (hb_interval_ms, hb_timeout_ms)
    groups = defaultdict(list)
    for hb_i, hb_t, lat in runs:
        groups[(hb_i, hb_t)].append(lat)

    rows = []
    for (hb_interval_ms, hb_timeout_ms), lats in groups.items():
        lats = np.array(lats)
        median_detection_ms = float(np.median(lats))
        q1, q3 = np.percentile(lats, [25, 75])
        iqr_detection_ms = float(q3 - q1)
        missed = hb_timeout_ms / hb_interval_ms
        rows.append({
            "hb_interval_ms": hb_interval_ms,
            "hb_timeout_ms": hb_timeout_ms,
            "missed": missed,
            "median_detection_ms": median_detection_ms,
            "iqr_detection_ms": iqr_detection_ms,
            "n_trials": len(lats),
        })

    rows.sort(key=lambda r: (r["hb_timeout_ms"], r["hb_interval_ms"]))

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Heatmap CSV: hb_timeout_ms, hb_interval_ms, median_detection_ms, iqr_detection_ms
    heatmap_path = OUT_DIR / "heatmap.csv"
    with open(heatmap_path, "w") as f:
        f.write("hb_timeout_ms,hb_interval_ms,median_detection_ms,iqr_detection_ms,n_trials\n")
        for r in rows:
            f.write(f"{r['hb_timeout_ms']},{r['hb_interval_ms']},{r['median_detection_ms']},{r['iqr_detection_ms']},{r['n_trials']}\n")
    print("Wrote", heatmap_path)

    # Scatter CSV: missed, hb_interval_ms, hb_timeout_ms, median_detection_ms, iqr_detection_ms
    scatter_path = OUT_DIR / "scatter.csv"
    with open(scatter_path, "w") as f:
        f.write("missed,hb_interval_ms,hb_timeout_ms,median_detection_ms,iqr_detection_ms,n_trials\n")
        for r in rows:
            f.write(f"{r['missed']},{r['hb_interval_ms']},{r['hb_timeout_ms']},{r['median_detection_ms']},{r['iqr_detection_ms']},{r['n_trials']}\n")
    print("Wrote", scatter_path)


if __name__ == "__main__":
    main()
