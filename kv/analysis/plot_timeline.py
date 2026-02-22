import json
from pathlib import Path
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
PLOTS = ROOT / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

def read_jsonl(path: Path):
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows

def main(run_id: str):
    run_dir = RUNS / run_id
    if not run_dir.exists():
        print(f"Error: run directory not found: {run_dir}")
        raise SystemExit(1)
    fault_path = run_dir / "fault.json"
    if not fault_path.exists():
        print(f"Error: fault.json not found in {run_dir}")
        raise SystemExit(1)
    fault = json.loads(fault_path.read_text())
    fault_ts = fault["ts_ms"]

    client = read_jsonl(run_dir / "client_events.jsonl")
    put = [e for e in client if e["op"] == "PUT"]

    xs = [(e["ts_ms"] - fault_ts) for e in put]
    ys = [1 if e["ok"] else 0 for e in put]

    plt.figure()
    plt.scatter(xs, ys)
    plt.xlabel("ms since leader kill")
    plt.ylabel("PUT success (1) / fail (0)")
    plt.title(f"Timeline: {run_id}")
    out = PLOTS / f"timeline_{run_id}.png"
    plt.savefig(out, dpi=200, bbox_inches="tight")
    print("saved:", out)

if __name__ == "__main__":
    # 例: python analysis/plot_timeline.py leader_crash_hb100_to500_...
    import sys
    if len(sys.argv) != 2:
        print("usage: python plot_timeline.py <run_id>")
        raise SystemExit(1)
    main(sys.argv[1])