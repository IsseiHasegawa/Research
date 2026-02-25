#!/usr/bin/env bash
# Build, sweep, aggregate, and plot in one go. Run from kv-2node-fd-spec/.
set -e
cd "$(dirname "$0")"

echo "==> Building..."
mkdir -p build && cd build && cmake -q .. && make -j4
cd ..

echo "==> Running sweep..."
python3 run_sweep.py

echo "==> Aggregating..."
python3 aggregate.py

echo "==> Plotting..."
python3 plot_results.py

echo "==> Done. See analysis_out/heatmap_plot.png and scatter_plot.png"
