#!/usr/bin/env python3
"""
Plot experiment results: scatter (missed vs median detection latency) and heatmap.
Reads analysis_out/heatmap.csv and scatter.csv; writes PNGs to analysis_out/ or plots/.
"""
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "analysis_out"
HEATMAP_CSV = OUT_DIR / "heatmap.csv"
SCATTER_CSV = OUT_DIR / "scatter.csv"


def load_heatmap():
    if not HEATMAP_CSV.exists():
        return None, None, None, None
    data = np.genfromtxt(HEATMAP_CSV, delimiter=",", dtype=float, skip_header=1)
    if data.size == 0 or data.ndim == 1:
        data = data.reshape(1, -1) if data.size else np.empty((0, 5))
    hb_timeout = data[:, 0]
    hb_interval = data[:, 1]
    median_det = data[:, 2]
    iqr_det = data[:, 3]
    return hb_timeout, hb_interval, median_det, iqr_det


def load_scatter():
    if not SCATTER_CSV.exists():
        return None, None, None, None
    data = np.genfromtxt(SCATTER_CSV, delimiter=",", dtype=float, skip_header=1)
    if data.size == 0 or data.ndim == 1:
        data = data.reshape(1, -1) if data.size else np.empty((0, 6))
    missed = data[:, 0]
    median_det = data[:, 3]
    iqr_det = data[:, 4]
    return missed, median_det, iqr_det, data


def main():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found. Install: python3 -m pip install matplotlib")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Scatter: missed vs median_detection_ms (primary research plot) ----
    missed, median_det, iqr_det, scatter_data = load_scatter()
    if missed is not None and len(missed) > 0:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.errorbar(
            missed, median_det, yerr=iqr_det / 2, fmt="o", capsize=5, capthick=1.5,
            color="steelblue", ecolor="gray", markersize=10, label="median ± IQR/2"
        )
        ax.set_xlabel("missed (= hb_timeout_ms / hb_interval_ms)")
        ax.set_ylabel("Median detection latency (ms)")
        ax.set_title("Failure detection latency vs missed heartbeats")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        scatter_path = OUT_DIR / "scatter_plot.png"
        fig.savefig(scatter_path, dpi=150)
        plt.close(fig)
        print(f"Saved {scatter_path}")

    # ---- Heatmap: (hb_interval_ms, hb_timeout_ms) -> median_detection_ms ----
    hb_timeout, hb_interval, median_det, iqr_det = load_heatmap()
    if hb_timeout is not None and len(hb_timeout) > 0:
        timeouts = np.unique(hb_timeout)
        intervals = np.unique(hb_interval)
        if len(timeouts) > 0 and len(intervals) > 0:
            Z = np.full((len(timeouts), len(intervals)), np.nan)
            for i, to in enumerate(timeouts):
                for j, iv in enumerate(intervals):
                    mask = (hb_timeout == to) & (hb_interval == iv)
                    if np.any(mask):
                        Z[i, j] = np.nanmean(median_det[mask])
            fig, ax = plt.subplots(figsize=(5, 4))
            im = ax.imshow(Z, aspect="auto", cmap="YlOrRd", origin="lower")
            ax.set_xticks(np.arange(len(intervals)))
            ax.set_yticks(np.arange(len(timeouts)))
            ax.set_xticklabels([int(x) for x in intervals])
            ax.set_yticklabels([int(x) for x in timeouts])
            ax.set_xlabel("hb_interval_ms")
            ax.set_ylabel("hb_timeout_ms")
            ax.set_title("Median detection latency (ms)")
            for i in range(len(timeouts)):
                for j in range(len(intervals)):
                    v = Z[i, j]
                    if not np.isnan(v):
                        ax.text(j, i, f"{v:.0f}", ha="center", va="center", color="black", fontsize=10)
            plt.colorbar(im, ax=ax, label="median_detection_ms")
            fig.tight_layout()
            heatmap_path = OUT_DIR / "heatmap_plot.png"
            fig.savefig(heatmap_path, dpi=150)
            plt.close(fig)
            print(f"Saved {heatmap_path}")
        else:
            # Fallback: bar chart when only a few points
            fig, ax = plt.subplots(figsize=(5, 4))
            labels = [f"i={int(iv)}\nt={int(to)}" for iv, to in zip(hb_interval, hb_timeout)]
            x = np.arange(len(labels))
            ax.bar(x, median_det, color="steelblue", alpha=0.8)
            ax.set_xticks(x)
            ax.set_xticklabels(labels)
            ax.set_ylabel("Median detection latency (ms)")
            ax.set_title("Median detection latency by (hb_interval_ms, hb_timeout_ms)")
            fig.tight_layout()
            heatmap_path = OUT_DIR / "heatmap_plot.png"
            fig.savefig(heatmap_path, dpi=150)
            plt.close(fig)
            print(f"Saved {heatmap_path}")

    if (missed is None or len(missed) == 0) and (hb_timeout is None or len(hb_timeout) == 0):
        print("No data in heatmap.csv or scatter.csv. Run aggregate.py after run_sweep.py.")


if __name__ == "__main__":
    main()
