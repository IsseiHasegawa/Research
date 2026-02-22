# Distributed KV Store - Run Guide

## Setup

### 1. C++ Build

```bash
cd kv
mkdir -p build
cd build
cmake ..
make
```

This produces `build/kvnode`.

### 2. Python Dependencies

```bash
cd kv
pip3 install -r experiments/requirements.txt
```

Required packages:
- `requests` (HTTP client)
- `pandas` (data analysis)
- `matplotlib` (visualization)

## How to Run

### Run Experiments

```bash
cd kv
python3 experiments/run_experiments.py
```

This script:
- Runs experiments across heartbeat intervals (50, 100, 200ms) and timeouts (200, 400, 800, 1200ms)
- Crashes the leader in each run and measures failure detection time and downtime
- Saves results under `runs/`

### Manual Node Startup

#### Leader (port 8001)

```bash
./build/kvnode \
  --id A \
  --port 8001 \
  --leader 1 \
  --peers "B@127.0.0.1:8002,C@127.0.0.1:8003" \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/A.jsonl
```

#### Follower B (port 8002)

```bash
./build/kvnode \
  --id B \
  --port 8002 \
  --leader 0 \
  --leader_addr 127.0.0.1:8001 \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/B.jsonl
```

#### Follower C (port 8003)

```bash
./build/kvnode \
  --id C \
  --port 8003 \
  --leader 0 \
  --leader_addr 127.0.0.1:8001 \
  --hb_interval 100 \
  --hb_timeout 500 \
  --log runs/test/C.jsonl
```

### API Examples

#### PUT (write to leader)

```bash
curl -X POST http://127.0.0.1:8001/put?rid=test-1 \
  -H "Content-Type: application/json" \
  -d '{"key": "x", "value": "hello"}'
```

#### GET (read from any node)

```bash
curl -X POST http://127.0.0.1:8002/get?rid=test-2 \
  -H "Content-Type: application/json" \
  -d '{"key": "x"}'
```

## Result Analysis

### 1. Extract Metrics

```bash
cd kv
python3 analysis/analyze.py
```

This generates `analysis_out/metrics.csv`.

### 2. Generate Heatmaps

```bash
python3 analysis/plot_heatmap.py
```

Output files:
- `plots/heatmap_downtime.png` - Downtime heatmap
- `plots/heatmap_detection.png` - Failure detection time heatmap

### 3. Timeline Visualization

**List available run_ids:**
```bash
ls runs/
```

**Generate timeline:**
```bash
python3 analysis/plot_timeline.py <run_id>
```

**Example:**
```bash
# Example: use one of the available run_ids
python3 analysis/plot_timeline.py leader_crash_hb100_to400_1771787322474
```

**Note:** Replace `<run_id>` with an actual run_id. Running with the literal string `<run_id>` will cause an error.

Output file:
- `plots/timeline_<run_id>.png` - PUT success/failure timeline

## Directory Structure

```
kv/
├── build/              # Build artifacts
│   └── kvnode          # Executable
├── src/                # C++ source code
├── vendor/             # Third-party libraries (httplib.h)
├── experiments/        # Experiment scripts
│   └── run_experiments.py
├── analysis/           # Analysis scripts
│   ├── analyze.py
│   ├── plot_heatmap.py
│   └── plot_timeline.py
├── runs/               # Experiment results (auto-generated)
│   └── <run_id>/
│       ├── meta.json
│       ├── fault.json
│       ├── client_events.jsonl
│       ├── A.jsonl, B.jsonl, C.jsonl
│       └── ...
├── analysis_out/       # Analysis output (auto-generated)
│   └── metrics.csv
└── plots/              # Plots (auto-generated)
    └── ...
```

## Troubleshooting

### Build Errors

- **nlohmann/json not found**: CMake FetchContent will download it automatically. Check your network connection.
- **httplib.h not found**: Ensure `vendor/httplib.h` exists.

### Runtime Errors

- **Port in use**: Check that no other process is using ports 8001–8003.
- **pandas/matplotlib not found**: Run `pip3 install -r experiments/requirements.txt`.

### Experiments Not Completing

- Each experiment takes about 6 seconds. Running many combinations will take longer.
- If processes are left running, use `pkill kvnode` to stop them.
