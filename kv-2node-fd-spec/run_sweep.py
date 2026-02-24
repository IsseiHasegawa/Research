#!/usr/bin/env python3
"""
Run multiple injector trials for a few (hb_interval_ms, hb_timeout_ms) settings,
then run the aggregator to produce heatmap.csv and scatter.csv.
Uses hb_timeout_ms in 200–300 ms range for reliable declaration.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INJECTOR = ROOT / "injector.py"
AGGREGATE = ROOT / "aggregate.py"
BIN = ROOT / "build" / "kvnode"

# (hb_interval_ms, hb_timeout_ms), trials per setting. Use timeout 200–300 for reliable declaration.
TRIALS = 5
SETTINGS = [
    (100, 200),
    (100, 300),
    (200, 200),
    (200, 300),
]


def main():
    if not BIN.exists():
        print(f"Build first: cd {ROOT} && mkdir -p build && cd build && cmake .. && make", file=sys.stderr)
        sys.exit(1)

    for hb_i, hb_t in SETTINGS:
        print(f"Running {TRIALS} trials hb_interval_ms={hb_i} hb_timeout_ms={hb_t} ...")
        for t in range(TRIALS):
            r = subprocess.run(
                [sys.executable, str(INJECTOR), "--hb_interval_ms", str(hb_i), "--hb_timeout_ms", str(hb_t)],
                cwd=str(ROOT),
            )
            if r.returncode != 0:
                print(f"  trial {t+1}/{TRIALS} exit {r.returncode} (detection may have timed out)")

    print("Aggregating ...")
    subprocess.run([sys.executable, str(AGGREGATE)], cwd=str(ROOT), check=True)
    print("Done. See analysis_out/heatmap.csv and analysis_out/scatter.csv")


if __name__ == "__main__":
    main()
