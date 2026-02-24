#!/usr/bin/env python3
"""
Failure injection script for 2-node FD experiment.
Starts B then A, warmup, records t_fail (wall ms), kills B, waits for declared_dead in A's log.
Writes injector.jsonl with same time base (wall clock) as A.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# Default paths relative to repo root (injector run from kv-2node-fd-spec/)
ROOT = Path(__file__).resolve().parent
BIN = ROOT / "build" / "kvnode"
RUNS_DIR = ROOT / "runs"
WARMUP_SEC = 2.5
# Wait for declared_dead: scale with hb_timeout so large timeouts don't time out prematurely.
WAIT_DECLARE_K = 6.0  # timeout_sec = max(2.0, K * hb_timeout_ms/1000 + 1.0)


def wall_ms():
    return int(time.time() * 1000)


def run_one(hb_interval_ms: int, hb_timeout_ms: int, run_dir: Path) -> dict:
    run_id = f"fd_run_{hb_interval_ms}_{hb_timeout_ms}_{wall_ms()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    log_a = run_dir / "A.jsonl"
    log_b = run_dir / "B.jsonl"

    env = os.environ.copy()
    env["RUN_ID"] = run_id

    # Start B (monitored) then A (detector). B in own process group so we can kill it reliably.
    proc_b = subprocess.Popen(
        [
            str(BIN),
            "--id", "B",
            "--port", "8002",
            "--role", "monitored",
            "--log_path", str(log_b),
            "--hb_interval_ms", str(hb_interval_ms),
            "--hb_timeout_ms", str(hb_timeout_ms),
            "--run_id", run_id,
        ],
        cwd=str(ROOT),
        stdout=(run_dir / "B.out").open("w"),
        stderr=subprocess.STDOUT,
        env=env,
    )
    time.sleep(0.3)

    proc_a = subprocess.Popen(
        [
            str(BIN),
            "--id", "A",
            "--port", "8001",
            "--role", "detector",
            "--peer_addr", "127.0.0.1:8002",
            "--log_path", str(log_a),
            "--hb_interval_ms", str(hb_interval_ms),
            "--hb_timeout_ms", str(hb_timeout_ms),
            "--run_id", run_id,
        ],
        cwd=str(ROOT),
        stdout=(run_dir / "A.out").open("w"),
        stderr=subprocess.STDOUT,
        env=env,
    )
    time.sleep(0.5)

    # Warmup
    time.sleep(WARMUP_SEC)

    # Check A is running (has logged at least one heartbeat)
    if log_a.exists():
        with open(log_a) as f:
            content = f.read()
        if "hb_ping_sent" not in content and "hb_ack_recv" not in content:
            # A likely failed to start (e.g. "bind failed" if port 8001 in use)
            if (run_dir / "A.out").exists():
                with open(run_dir / "A.out") as f:
                    err = f.read().strip()
                print(f"Warning: A may have failed to start. Check {run_dir / 'A.out'}: {err[:200]}", file=sys.stderr)
    else:
        print(f"Warning: A log not found at {log_a}. Is port 8001 free? Check {run_dir / 'A.out'}", file=sys.stderr)

    # Record t_fail and kill B
    t_fail = wall_ms()
    injector_path = run_dir / "injector.jsonl"
    with open(injector_path, "w") as f:
        f.write(json.dumps({
            "event": "run_start",
            "run_id": run_id,
            "ts_ms": t_fail - int(WARMUP_SEC * 1000),
            "hb_interval_ms": hb_interval_ms,
            "hb_timeout_ms": hb_timeout_ms,
        }) + "\n")
        f.write(json.dumps({
            "event": "kill_b",
            "run_id": run_id,
            "ts_ms": t_fail,
            "hb_interval_ms": hb_interval_ms,
            "hb_timeout_ms": hb_timeout_ms,
        }) + "\n")

    proc_b.send_signal(signal.SIGKILL)
    try:
        proc_b.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc_b.kill()
        proc_b.wait()
    # Ensure B is really dead so A's connection closes
    time.sleep(0.3)

    # Wait for A to log declared_dead; scale with hb_timeout_ms so sweeps with large timeouts succeed.
    timeout_sec = max(2.0, WAIT_DECLARE_K * (hb_timeout_ms / 1000.0) + 1.0)
    deadline = time.time() + timeout_sec
    t_detect = None
    while time.time() < deadline:
        if not log_a.exists():
            time.sleep(0.02)
            continue
        try:
            with open(log_a) as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        if obj.get("event") == "declared_dead" and obj.get("run_id") == run_id:
                            ts = obj.get("ts_ms")
                            # Only count declaration after we killed B (avoid warmup false positive)
                            if ts is not None and ts >= t_fail - 100:
                                t_detect = ts
                                break
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass
        if t_detect is not None:
            break
        time.sleep(0.02)
    # Final re-read (log flush / filesystem)
    if t_detect is None:
        for _ in range(25):  # 0.5s more
            time.sleep(0.02)
            if log_a.exists():
                with open(log_a) as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            if obj.get("event") == "declared_dead" and obj.get("run_id") == run_id:
                                ts = obj.get("ts_ms")
                                if ts is not None and ts >= t_fail - 100:
                                    t_detect = ts
                                    break
                        except json.JSONDecodeError:
                            continue
                if t_detect is not None:
                    break

    detection_latency_ms = (t_detect - t_fail) if t_detect is not None else None

    with open(injector_path, "a") as f:
        f.write(json.dumps({
            "event": "declared_dead",
            "run_id": run_id,
            "ts_ms": t_detect,
            "hb_interval_ms": hb_interval_ms,
            "hb_timeout_ms": hb_timeout_ms,
            "detection_latency_ms": detection_latency_ms,
        }) + "\n")

    # Stop A
    proc_a.terminate()
    try:
        proc_a.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc_a.kill()
        proc_a.wait()

    return {
        "run_id": run_id,
        "hb_interval_ms": hb_interval_ms,
        "hb_timeout_ms": hb_timeout_ms,
        "t_fail": t_fail,
        "t_detect": t_detect,
        "detection_latency_ms": detection_latency_ms,
    }


def main():
    import argparse
    p = argparse.ArgumentParser(description="Run one FD experiment: start A&B, kill B, record detection latency")
    p.add_argument("--hb_interval_ms", type=int, default=100)
    p.add_argument("--hb_timeout_ms", type=int, default=400)
    p.add_argument("--run_dir", type=Path, default=None, help="Output run dir (default: runs/fd_run_<params>_<ts>)")
    args = p.parse_args()

    if not BIN.exists():
        print(f"Binary not found: {BIN}. Run: cd {ROOT} && mkdir -p build && cd build && cmake .. && make", file=sys.stderr)
        sys.exit(1)

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"fd_run_{args.hb_interval_ms}_{args.hb_timeout_ms}_{wall_ms()}"
    run_dir = args.run_dir or (RUNS_DIR / run_id)

    result = run_one(args.hb_interval_ms, args.hb_timeout_ms, run_dir)
    print(json.dumps(result, indent=2))
    if result["detection_latency_ms"] is None:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
