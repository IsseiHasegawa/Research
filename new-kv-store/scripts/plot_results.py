#!/usr/bin/env python3
"""
plot_results.py — Generate visualizations from experiment results.

Reads CSV files produced by compute_metrics.py and generates:
  1. Heatmap: detection latency vs (hb_interval, hb_timeout)
  2. Scatter: detection latency vs missed heartbeats
  3. Bar chart: write latency by replication mode
  4. Bar chart: throughput by replication mode
  5. Grouped bar: metrics comparison across fault types
  6. Line graph: detection latency vs heartbeat interval (grouped by repl mode)

Outputs PNGs to output/plots/.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np


def load_csv(path: Path) -> list[dict]:
    """Load a CSV file into a list of dicts."""
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def safe_float(val, default=None):
    """Convert to float safely."""
    if val is None or val == "" or val == "None":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Plot experiment results")
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parent.parent / "output",
                        help="Output directory (contains results/)")
    args = parser.parse_args()

    results_dir = args.output / "results"
    plots_dir = args.output / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.colors import Normalize
    except ImportError:
        print("Error: matplotlib not found. Install: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    # Use a clean style
    plt.rcParams.update({
        "figure.facecolor": "white",
        "axes.facecolor": "#f8f9fa",
        "axes.grid": True,
        "grid.alpha": 0.3,
        "font.size": 11,
    })

    # ── Load data ─────────────────────────────────────────────────────────────
    summary = load_csv(results_dir / "summary.csv")
    aggregate = load_csv(results_dir / "aggregate.csv")
    heatmap_data = load_csv(results_dir / "heatmap.csv")
    scatter_data = load_csv(results_dir / "scatter.csv")

    plots_generated = 0

    # ── 1. Scatter: missed heartbeats vs detection latency ────────────────────
    if scatter_data:
        missed = [safe_float(r["missed"]) for r in scatter_data]
        median_det = [safe_float(r["median_detection_ms"]) for r in scatter_data]
        iqr_det = [safe_float(r["iqr_detection_ms"], 0) for r in scatter_data]
        labels = [r.get("repl_mode", "") for r in scatter_data]

        if all(v is not None for v in missed) and all(v is not None for v in median_det):
            fig, ax = plt.subplots(figsize=(8, 5))

            # Color by replication mode
            colors_map = {"none": "#3498db", "sync": "#e74c3c", "async": "#2ecc71"}
            colors = [colors_map.get(l, "#95a5a6") for l in labels]

            ax.errorbar(missed, median_det,
                        yerr=[i / 2 if i else 0 for i in iqr_det],
                        fmt="none", ecolor="#bdc3c7", capsize=4, capthick=1.2,
                        zorder=1)
            scatter = ax.scatter(missed, median_det, c=colors, s=100,
                                 edgecolors="white", linewidth=1.5, zorder=2)

            # Add legend for replication modes
            for mode, color in colors_map.items():
                ax.scatter([], [], c=color, s=80, edgecolors="white",
                           linewidth=1.5, label=mode)
            ax.legend(title="Replication Mode", loc="upper left")

            ax.set_xlabel("Missed Heartbeats (timeout / interval)")
            ax.set_ylabel("Median Detection Latency (ms)")
            ax.set_title("Failure Detection Latency vs Missed Heartbeats")
            fig.tight_layout()
            fig.savefig(plots_dir / "scatter_detection_latency.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved scatter_detection_latency.png")

    # ── 2. Heatmap: detection latency by (hb_interval, hb_timeout) ───────────
    if heatmap_data:
        timeouts = sorted(set(safe_float(r["hb_timeout_ms"]) for r in heatmap_data))
        intervals = sorted(set(safe_float(r["hb_interval_ms"]) for r in heatmap_data))
        timeouts = [t for t in timeouts if t is not None]
        intervals = [i for i in intervals if i is not None]

        if len(timeouts) > 1 or len(intervals) > 1:
            Z = np.full((len(timeouts), len(intervals)), np.nan)
            for r in heatmap_data:
                to_val = safe_float(r["hb_timeout_ms"])
                iv_val = safe_float(r["hb_interval_ms"])
                med_val = safe_float(r["median_detection_ms"])
                if to_val is not None and iv_val is not None and med_val is not None:
                    ti = timeouts.index(to_val)
                    ii = intervals.index(iv_val)
                    if np.isnan(Z[ti, ii]):
                        Z[ti, ii] = med_val
                    else:
                        Z[ti, ii] = (Z[ti, ii] + med_val) / 2  # avg across repl modes

            fig, ax = plt.subplots(figsize=(7, 5))
            im = ax.imshow(Z, aspect="auto", cmap="YlOrRd", origin="lower",
                           interpolation="nearest")
            ax.set_xticks(range(len(intervals)))
            ax.set_yticks(range(len(timeouts)))
            ax.set_xticklabels([int(x) for x in intervals])
            ax.set_yticklabels([int(x) for x in timeouts])
            ax.set_xlabel("Heartbeat Interval (ms)")
            ax.set_ylabel("Heartbeat Timeout (ms)")
            ax.set_title("Median Detection Latency (ms)")

            for i in range(len(timeouts)):
                for j in range(len(intervals)):
                    v = Z[i, j]
                    if not np.isnan(v):
                        ax.text(j, i, f"{v:.0f}", ha="center", va="center",
                                color="black", fontsize=11, fontweight="bold")

            plt.colorbar(im, ax=ax, label="Detection Latency (ms)")
            fig.tight_layout()
            fig.savefig(plots_dir / "heatmap_detection_latency.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved heatmap_detection_latency.png")

    # ── 3. Bar chart: write latency by replication mode ───────────────────────
    if aggregate:
        by_repl = defaultdict(list)
        for r in aggregate:
            mode = r.get("repl_mode", "unknown")
            med = safe_float(r.get("write_latency_median_us_median"))
            if med is not None:
                by_repl[mode].append(med)

        if by_repl:
            modes = sorted(by_repl.keys())
            means = [np.mean(by_repl[m]) for m in modes]
            stds = [np.std(by_repl[m]) if len(by_repl[m]) > 1 else 0 for m in modes]

            colors = {"none": "#3498db", "sync": "#e74c3c", "async": "#2ecc71"}
            bar_colors = [colors.get(m, "#95a5a6") for m in modes]

            fig, ax = plt.subplots(figsize=(6, 4))
            bars = ax.bar(range(len(modes)), means, yerr=stds,
                          color=bar_colors, edgecolor="white", linewidth=1.5,
                          capsize=5, alpha=0.9)
            ax.set_xticks(range(len(modes)))
            ax.set_xticklabels(modes)
            ax.set_xlabel("Replication Mode")
            ax.set_ylabel("Median Write Latency (μs)")
            ax.set_title("Write Latency by Replication Mode")

            for bar, val in zip(bars, means):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(means) * 0.02,
                        f"{val:.0f}μs", ha="center", va="bottom", fontsize=10)

            fig.tight_layout()
            fig.savefig(plots_dir / "write_latency_by_repl.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved write_latency_by_repl.png")

    # ── 4. Bar chart: throughput by replication mode ──────────────────────────
    if aggregate:
        by_repl_tp = defaultdict(list)
        for r in aggregate:
            mode = r.get("repl_mode", "unknown")
            tp = safe_float(r.get("throughput_ops_sec_median"))
            if tp is not None:
                by_repl_tp[mode].append(tp)

        if by_repl_tp:
            modes = sorted(by_repl_tp.keys())
            means = [np.mean(by_repl_tp[m]) for m in modes]

            colors = {"none": "#3498db", "sync": "#e74c3c", "async": "#2ecc71"}
            bar_colors = [colors.get(m, "#95a5a6") for m in modes]

            fig, ax = plt.subplots(figsize=(6, 4))
            bars = ax.bar(range(len(modes)), means,
                          color=bar_colors, edgecolor="white", linewidth=1.5,
                          alpha=0.9)
            ax.set_xticks(range(len(modes)))
            ax.set_xticklabels(modes)
            ax.set_xlabel("Replication Mode")
            ax.set_ylabel("Throughput (ops/sec)")
            ax.set_title("Throughput by Replication Mode")

            for bar, val in zip(bars, means):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(means) * 0.02,
                        f"{val:.0f}", ha="center", va="bottom", fontsize=10)

            fig.tight_layout()
            fig.savefig(plots_dir / "throughput_by_repl.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved throughput_by_repl.png")

    # ── 5. Grouped comparison: detection latency + downtime by config ────────
    if aggregate:
        configs = [r.get("config", "") for r in aggregate]
        det_lat = [safe_float(r.get("detection_latency_ms_median"), 0) for r in aggregate]
        downtime = [safe_float(r.get("downtime_ms_median"), 0) for r in aggregate]

        if any(d > 0 for d in det_lat):
            fig, ax = plt.subplots(figsize=(max(8, len(configs) * 1.2), 5))
            x = np.arange(len(configs))
            width = 0.35

            bars1 = ax.bar(x - width / 2, det_lat, width, label="Detection Latency (ms)",
                           color="#3498db", alpha=0.9, edgecolor="white")
            bars2 = ax.bar(x + width / 2, downtime, width, label="Downtime (ms)",
                           color="#e74c3c", alpha=0.9, edgecolor="white")

            ax.set_xlabel("Configuration")
            ax.set_ylabel("Time (ms)")
            ax.set_title("Detection Latency vs Downtime by Configuration")
            ax.set_xticks(x)
            ax.set_xticklabels(configs, rotation=45, ha="right", fontsize=9)
            ax.legend()
            fig.tight_layout()
            fig.savefig(plots_dir / "detection_vs_downtime.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved detection_vs_downtime.png")

    # ── 6. Line: detection latency vs hb_interval, grouped by repl_mode ──────
    if aggregate:
        by_repl_line = defaultdict(lambda: {"intervals": [], "latencies": []})
        for r in aggregate:
            mode = r.get("repl_mode", "unknown")
            hb_i = safe_float(r.get("hb_interval_ms"))
            lat = safe_float(r.get("detection_latency_ms_median"))
            if hb_i is not None and lat is not None:
                by_repl_line[mode]["intervals"].append(hb_i)
                by_repl_line[mode]["latencies"].append(lat)

        if by_repl_line:
            colors = {"none": "#3498db", "sync": "#e74c3c", "async": "#2ecc71"}
            markers = {"none": "o", "sync": "s", "async": "^"}

            fig, ax = plt.subplots(figsize=(7, 5))
            for mode, data in sorted(by_repl_line.items()):
                # Sort by interval
                pairs = sorted(zip(data["intervals"], data["latencies"]))
                ivs, lats = zip(*pairs) if pairs else ([], [])
                ax.plot(ivs, lats,
                        marker=markers.get(mode, "o"),
                        color=colors.get(mode, "#95a5a6"),
                        linewidth=2, markersize=8,
                        label=f"repl={mode}")

            ax.set_xlabel("Heartbeat Interval (ms)")
            ax.set_ylabel("Median Detection Latency (ms)")
            ax.set_title("Detection Latency vs Heartbeat Interval")
            ax.legend()
            fig.tight_layout()
            fig.savefig(plots_dir / "detection_vs_interval.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved detection_vs_interval.png")

    # ── 7. Per-trial latency distribution (box plot) ─────────────────────────
    if summary:
        by_config_latency = defaultdict(list)
        for r in summary:
            cfg = r.get("config", "unknown")
            lat = safe_float(r.get("detection_latency_ms"))
            if lat is not None and lat >= 0:
                by_config_latency[cfg].append(lat)

        if by_config_latency:
            configs = sorted(by_config_latency.keys())
            data = [by_config_latency[c] for c in configs]

            fig, ax = plt.subplots(figsize=(max(8, len(configs) * 1.2), 5))
            bp = ax.boxplot(data, patch_artist=True, notch=True)
            for patch in bp["boxes"]:
                patch.set_facecolor("#3498db")
                patch.set_alpha(0.7)
            ax.set_xticklabels(configs, rotation=45, ha="right", fontsize=9)
            ax.set_xlabel("Configuration")
            ax.set_ylabel("Detection Latency (ms)")
            ax.set_title("Detection Latency Distribution by Configuration")
            fig.tight_layout()
            fig.savefig(plots_dir / "detection_latency_boxplot.png", dpi=150)
            plt.close(fig)
            plots_generated += 1
            print(f"Saved detection_latency_boxplot.png")

    if plots_generated == 0:
        print("No data available for plotting. Run experiments first.")
    else:
        print(f"\nGenerated {plots_generated} plots in {plots_dir}")


if __name__ == "__main__":
    main()
