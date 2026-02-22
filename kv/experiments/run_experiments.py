import json
import time
import signal
import subprocess
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent  # kv/
BIN  = ROOT / "build" / "kvnode"

RUNS_DIR = ROOT / "runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

def start_node(node_id, port, is_leader, run_dir: Path,
               peers="", leader_addr="",
               hb_interval=100, hb_timeout=500):
    log_path = run_dir / f"{node_id}.jsonl"
    args = [
        str(BIN),
        "--id", node_id,
        "--port", str(port),
        "--leader", "1" if is_leader else "0",
        "--hb_interval", str(hb_interval),
        "--hb_timeout", str(hb_timeout),
        "--log", str(log_path),
    ]
    if peers:
        args += ["--peers", peers]
    if leader_addr:
        args += ["--leader_addr", leader_addr]

    # Save stdout to run_dir
    out = (run_dir / f"{node_id}.out").open("w")
    p = subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT, text=True)
    return p

def safe_kill(p):
    if p is None:
        return
    if p.poll() is not None:
        return
    p.send_signal(signal.SIGKILL)

def put(port, key, value, rid):
    url = f"http://127.0.0.1:{port}/put?rid={rid}"
    r = requests.post(url, json={"key": key, "value": value}, timeout=0.5)
    return r.status_code, r.text

def get(port, key, rid):
    url = f"http://127.0.0.1:{port}/get?rid={rid}"
    r = requests.post(url, json={"key": key}, timeout=0.5)
    return r.status_code, r.text

def run_leader_crash(hb_interval=100, hb_timeout=500,
                     crash_at_sec=2.0, duration_sec=6.0):
    run_id = f"leader_crash_hb{hb_interval}_to{hb_timeout}_{int(time.time()*1000)}"
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "run_id": run_id,
        "scenario": "leader_crash",
        "hb_interval_ms": hb_interval,
        "hb_timeout_ms": hb_timeout,
        "crash_at_sec": crash_at_sec,
        "duration_sec": duration_sec,
        "started_ts_ms": int(time.time()*1000),
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    leader = start_node("A", 8001, True,  run_dir,
                        peers="B@127.0.0.1:8002,C@127.0.0.1:8003",
                        hb_interval=hb_interval, hb_timeout=hb_timeout)
    b = start_node("B", 8002, False, run_dir,
                   leader_addr="127.0.0.1:8001",
                   hb_interval=hb_interval, hb_timeout=hb_timeout)
    c = start_node("C", 8003, False, run_dir,
                   leader_addr="127.0.0.1:8001",
                   hb_interval=hb_interval, hb_timeout=hb_timeout)

    time.sleep(0.6)  # warmup

    t0 = time.time()
    crashed = False
    fault = None

    # client event log (for precise downtime calc later)
    events = []

    while True:
        t = time.time() - t0

        if (not crashed) and t >= crash_at_sec:
            crashed = True
            fault = {"fault": "leader_kill", "t_sec": t, "ts_ms": int(time.time()*1000)}
            (run_dir / "fault.json").write_text(json.dumps(fault, indent=2))
            safe_kill(leader)

        rid = f"{run_id}-{int(t*1000)}"

        # PUT to leader
        put_ok = False
        try:
            sc, _ = put(8001, "x", f"v{int(t*1000)}", rid)
            put_ok = (sc == 200)
        except Exception:
            put_ok = False
        events.append({"ts_ms": int(time.time()*1000), "op": "PUT", "ok": put_ok})

        # GET from follower B
        get_ok = False
        try:
            sc, body = get(8002, "x", rid + "-g")
            get_ok = (sc == 200)
        except Exception:
            get_ok = False
        events.append({"ts_ms": int(time.time()*1000), "op": "GET@B", "ok": get_ok})

        if t >= duration_sec:
            break
        time.sleep(0.05)

    # stop followers
    safe_kill(b)
    safe_kill(c)

    (run_dir / "client_events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")

    summary = {
        "num_events": len(events),
        "put_ok": sum(1 for e in events if e["op"] == "PUT" and e["ok"]),
        "put_fail": sum(1 for e in events if e["op"] == "PUT" and (not e["ok"])),
        "get_ok": sum(1 for e in events if e["op"] == "GET@B" and e["ok"]),
        "get_fail": sum(1 for e in events if e["op"] == "GET@B" and (not e["ok"])),
        "crashed": crashed,
    }
    (run_dir / "client_summary.json").write_text(json.dumps(summary, indent=2))

    return run_dir

def main():
    hb_intervals = [50, 100, 200]
    hb_timeouts  = [200, 400, 800, 1200]

    for hi in hb_intervals:
        for ht in hb_timeouts:
            print("run:", hi, ht)
            run_leader_crash(hb_interval=hi, hb_timeout=ht)

if __name__ == "__main__":
    main()