# Minimal 2-Node KV Store — Failure Detection

Standalone implementation of a 2-node in-memory KV store with heartbeat-based failure detection for measuring detection latency. **No dependency on the `kv` folder.**

## Contents

- **[SPEC.md](SPEC.md)** — Design doc (sections 1–10).
- **[VERIFICATION.md](VERIFICATION.md)** — Post-implementation checklist.
- **C++ node** — Single binary `kvnode` (detector A or monitored B).
- **injector.py** — Starts A & B, kills B, records t_fail and t_detect.
- **aggregate.py** — Produces heatmap.csv and scatter.csv from runs.
- **run_sweep.py** — Runs multiple trials and then aggregate.

## Build

```bash
cd kv-2node-fd-spec
mkdir -p build && cd build
cmake ..
make
```

Binary: `build/kvnode`.

## Run nodes manually

**Terminal 1 — B (monitored):**
```bash
./build/kvnode --id B --port 8002 --role monitored --log_path /tmp/b.jsonl --hb_interval_ms 100 --hb_timeout_ms 200 --run_id manual
```

**Terminal 2 — A (detector):**
```bash
./build/kvnode --id A --port 8001 --role detector --peer_addr 127.0.0.1:8002 --log_path /tmp/a.jsonl --hb_interval_ms 100 --hb_timeout_ms 200 --run_id manual
```

Then kill B (Ctrl+C or `kill <B_PID>`); A should log `declared_dead` within about one timeout.

## Failure injection and aggregation

**Single run:**
```bash
python3 injector.py --hb_interval_ms 100 --hb_timeout_ms 200
```
Output: run dir under `runs/`, and JSON with `t_fail`, `t_detect`, `detection_latency_ms`. Exit 0 if detection was seen.

**Parameter sweep (multiple trials, then aggregate):**
```bash
pip install -r requirements.txt   # numpy
python3 run_sweep.py
```
Writes `analysis_out/heatmap.csv` and `analysis_out/scatter.csv`.

**Aggregate existing runs only:**
```bash
python3 aggregate.py
```

## Notes

- **Time base:** Wall clock (ms since epoch) for logs and injector so t_fail and t_detect are comparable.
- **Recommended hb_timeout_ms:** 200–300 ms works reliably with the injector; larger timeouts may require a longer injector wait or different environment.
- **Plots:** Use heatmap.csv (rows=hb_timeout_ms, cols=hb_interval_ms, value=median_detection_ms) and scatter.csv (missed, median_detection_ms, etc.) with your preferred tool (e.g. pandas + matplotlib).
