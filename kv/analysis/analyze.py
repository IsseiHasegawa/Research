"""
Extract metrics from experiment runs and write metrics.csv for heatmap plots.
"""
import json
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
OUT_DIR = ROOT / "analysis_out"
METRICS_CSV = OUT_DIR / "metrics.csv"


def read_jsonl(path: Path):
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def analyze_run(run_dir: Path) -> Optional[dict]:
    """Extract metrics from a single run. Returns None if run is incomplete."""
    meta_path = run_dir / "meta.json"
    fault_path = run_dir / "fault.json"
    client_path = run_dir / "client_events.jsonl"

    if not meta_path.exists() or not fault_path.exists() or not client_path.exists():
        return None

    meta = json.loads(meta_path.read_text())
    fault = json.loads(fault_path.read_text())
    client = read_jsonl(client_path)

    fault_ts = fault.get("ts_ms")
    if fault_ts is None:
        return None

    put_events = [e for e in client if e["op"] == "PUT"]
    put_failures = [e for e in put_events if not e["ok"]]

    # Downtime: time from first PUT failure to last PUT failure (ms)
    if put_failures:
        downtime_ms = put_failures[-1]["ts_ms"] - put_failures[0]["ts_ms"]
    else:
        downtime_ms = 0

    # Detection time: first fd_leader_check with dead=true or fd_state_change (peer_id=leader, to=Dead)
    detection_ts = None
    for node in ["B", "C", "A"]:
        log_path = run_dir / f"{node}.jsonl"
        if not log_path.exists():
            continue
        for ev in read_jsonl(log_path):
            if ev.get("type") == "fd_leader_check" and ev.get("dead") is True:
                detection_ts = ev["ts_ms"]
                break
            if ev.get("type") == "fd_state_change" and ev.get("to") == "Dead" and ev.get("peer_id") == "leader":
                detection_ts = ev["ts_ms"]
                break
        if detection_ts is not None:
            break

    detection_ms = (detection_ts - fault_ts) if detection_ts else None
    # Negative detection_ms = false positive (detected "dead" before fault injection, e.g. at startup)
    if detection_ms is not None and detection_ms < 0:
        detection_ms = None  # Treat as invalid / not measurable

    return {
        "run_id": meta.get("run_id", run_dir.name),
        "scenario": meta.get("scenario", "unknown"),
        "hb_interval_ms": meta.get("hb_interval_ms"),
        "hb_timeout_ms": meta.get("hb_timeout_ms"),
        "downtime_ms": downtime_ms,
        "detection_ms": detection_ms,
        "put_ok": sum(1 for e in put_events if e["ok"]),
        "put_fail": len(put_failures),
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    for run_dir in sorted(RUNS.iterdir()):
        if not run_dir.is_dir():
            continue
        r = analyze_run(run_dir)
        if r is not None:
            rows.append(r)

    if not rows:
        print("No valid runs found")
        return

    # Write CSV
    import csv
    fieldnames = ["run_id", "scenario", "hb_interval_ms", "hb_timeout_ms", "downtime_ms", "detection_ms", "put_ok", "put_fail"]
    with open(METRICS_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {METRICS_CSV} ({len(rows)} runs)")


if __name__ == "__main__":
    main()
