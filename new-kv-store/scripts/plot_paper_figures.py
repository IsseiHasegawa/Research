#!/usr/bin/env python3
"""
Generate publication-quality figures from experiment results.

Requirements implemented:
- Multiple figure types (line, scatter, heatmap, bar, additional trade-off plot)
- Strict colorblind-safe palette
- Numeric annotation on every plotted data point/cell/bar
- Regression equations shown on line plots
- Reproducible output with deterministic formatting
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.transforms import Bbox


# Strict palette requested by user
COLORS = {
    "blue": "#0072B2",
    "orange": "#E69F00",
    "green": "#009E73",
    "red": "#D55E00",
    "purple": "#CC79A7",
}

MODE_COLORS = {
    "none": COLORS["blue"],
    "sync": COLORS["orange"],
    "async": COLORS["green"],
}

FD_MARKERS = {"fixed": "o", "phi": "s"}
MODE_MARKERS = {"none": "o", "sync": "s", "async": "^"}

PAPER_STYLE = {
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.color": "#DDDDDD",
    "grid.linestyle": "--",
    "grid.linewidth": 0.7,
    "grid.alpha": 0.8,
    "font.size": 12,
    "axes.titlesize": 15,
    "axes.labelsize": 13,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11,
    "legend.fontsize": 11,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
}


def annotate_points(
    ax: plt.Axes,
    xs: Iterable[float],
    ys: Iterable[float],
    labels: Iterable[str],
    base_offset: Tuple[int, int] = (8, 8),
) -> None:
    """
    Place labels while minimizing overlap with points and existing labels.
    Keep labels inside the plotting area.
    """
    x_list = list(xs)
    y_list = list(ys)
    label_list = list(labels)
    if not x_list:
        return

    # Draw once so renderer and transform extents are valid.
    fig = ax.figure
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    point_pixels = ax.transData.transform(np.column_stack([x_list, y_list]))

    # Candidate text offsets (in points), from near to far.
    candidates = [
        (10, 10), (10, -12), (-12, 10), (-12, -12),
        (0, 14), (14, 0), (-14, 0), (0, -14),
        (18, 12), (18, -14), (-18, 12), (-18, -14),
        (22, 0), (0, 22), (-22, 0), (0, -22),
        (26, 14), (26, -16), (-26, 14), (-26, -16),
        (0, 28), (0, -28), (30, 0), (-30, 0),
    ]
    candidates = [(dx + base_offset[0] // 4, dy + base_offset[1] // 4) for dx, dy in candidates]

    placed_boxes: List[Bbox] = []
    axes_box = ax.get_window_extent(renderer=renderer)
    margin = 4.0

    def bbox_too_close_to_points(bbox: Bbox, min_gap_px: float = 10.0) -> bool:
        expanded = Bbox.from_extents(
            bbox.x0 - min_gap_px, bbox.y0 - min_gap_px, bbox.x1 + min_gap_px, bbox.y1 + min_gap_px
        )
        for px, py in point_pixels:
            if expanded.contains(px, py):
                return True
        return False

    def bbox_inside_axes(bbox: Bbox) -> bool:
        return (
            bbox.x0 >= axes_box.x0 + margin
            and bbox.x1 <= axes_box.x1 - margin
            and bbox.y0 >= axes_box.y0 + margin
            and bbox.y1 <= axes_box.y1 - margin
        )

    def overlap_area(a: Bbox, b: Bbox) -> float:
        x_overlap = max(0.0, min(a.x1, b.x1) - max(a.x0, b.x0))
        y_overlap = max(0.0, min(a.y1, b.y1) - max(a.y0, b.y0))
        return x_overlap * y_overlap

    for i, (x, y, label) in enumerate(zip(x_list, y_list, label_list)):
        placed = False
        # Cycle candidate order by index for better spatial spread.
        ordered = candidates[i % len(candidates):] + candidates[: i % len(candidates)]
        best_fallback = None

        for dx, dy in ordered:
            ann = ax.annotate(
                label,
                (x, y),
                textcoords="offset points",
                xytext=(dx, dy),
                ha="left" if dx >= 0 else "right",
                va="bottom" if dy >= 0 else "top",
                fontsize=9,
                color="black",
                bbox={"boxstyle": "round,pad=0.18", "fc": "white", "ec": "none", "alpha": 0.9},
                arrowprops={"arrowstyle": "-", "lw": 0.6, "color": "#666666", "alpha": 0.7},
                zorder=5,
            )
            bbox = ann.get_window_extent(renderer=renderer).expanded(1.05, 1.2)
            label_overlap = any(bbox.overlaps(pb) for pb in placed_boxes)
            point_overlap = bbox_too_close_to_points(bbox)
            in_axes = bbox_inside_axes(bbox)

            if label_overlap or point_overlap or not in_axes:
                # Keep best fallback candidate (minimum overlap penalty) for hard clusters.
                penalty = 0.0
                for pb in placed_boxes:
                    penalty += overlap_area(bbox, pb)
                if not in_axes:
                    penalty += 1e6
                if point_overlap:
                    penalty += 5e5
                if (best_fallback is None) or (penalty < best_fallback[0]):
                    best_fallback = (penalty, dx, dy)
                ann.remove()
                continue

            placed_boxes.append(bbox)
            placed = True
            break

        if not placed:
            # Fallback: use least-overlap candidate, still aiming to remain inside axes.
            if best_fallback is not None:
                _, dx, dy = best_fallback
                ha = "left" if dx >= 0 else "right"
                va = "bottom" if dy >= 0 else "top"
            else:
                dx, dy = (26, 18)
                ha, va = ("left", "bottom")
            ann = ax.annotate(
                label,
                (x, y),
                textcoords="offset points",
                xytext=(dx, dy),
                ha=ha,
                va=va,
                fontsize=9,
                color="black",
                bbox={"boxstyle": "round,pad=0.18", "fc": "white", "ec": "none", "alpha": 0.9},
                arrowprops={"arrowstyle": "-", "lw": 0.6, "color": "#666666", "alpha": 0.7},
                zorder=5,
            )
            placed_boxes.append(ann.get_window_extent(renderer=renderer).expanded(1.05, 1.2))


def save_figure(fig: plt.Figure, path_stem: Path, caption: str) -> None:
    path_stem.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path_stem.with_suffix(".png"))
    fig.savefig(path_stem.with_suffix(".pdf"))
    with open(path_stem.with_suffix(".caption.txt"), "w", encoding="utf-8") as f:
        f.write(caption + "\n")
    plt.close(fig)


def load_data(results_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary = pd.read_csv(results_dir / "summary.csv")
    aggregate = pd.read_csv(results_dir / "aggregate.csv")
    return summary, aggregate


def baseline_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Focus on non-zipf crash rows for clean parameter comparisons.
    """
    return df[
        (df["fault_type"] == "crash")
        & (df["workload_zipf_alpha"].fillna(0.0) == 0.0)
    ].copy()


def figure1_line_latency_vs_hb_interval(agg: pd.DataFrame, out_dir: Path) -> str:
    """
    Figure 1: detection latency vs heartbeat interval, lines by replication mode.
    Includes linear regression equation per line.
    """
    df = baseline_filter(agg)
    df = df[df["fd_algo"] == "fixed"]

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    equations = []
    points: List[Dict[str, float | str]] = []

    for mode in ["none", "sync", "async"]:
        sub = df[df["repl_mode"] == mode].sort_values("hb_interval_ms")
        if sub.empty:
            continue
        x = sub["hb_interval_ms"].astype(float).to_numpy()
        y = sub["detection_latency_ms_median"].astype(float).to_numpy()

        ax.plot(
            x,
            y,
            color=MODE_COLORS[mode],
            marker=MODE_MARKERS[mode],
            linewidth=2.5,
            markersize=9,
            markeredgecolor="black",
            markeredgewidth=0.8,
            label=f"{mode}",
            zorder=3,
        )
        for xv, yv in zip(x, y):
            points.append({"mode": mode, "x": float(xv), "y": float(yv)})

        if len(x) >= 2:
            m, b = np.polyfit(x, y, 1)
            equations.append((mode, m, b))
            x_fit = np.linspace(x.min(), x.max(), 100)
            y_fit = m * x_fit + b
            ax.plot(
                x_fit,
                y_fit,
                color=MODE_COLORS[mode],
                linestyle=":",
                linewidth=1.4,
                alpha=0.85,
                zorder=2,
            )

    # Manual annotation placement to avoid overlap, especially near high x (~200 ms).
    # Base offsets for each mode.
    mode_offsets: Dict[str, Tuple[int, int, str, str]] = {
        "none": (10, -14, "left", "top"),
        "sync": (10, 10, "left", "bottom"),
        "async": (-12, 10, "right", "bottom"),
    }

    # For high x values, enforce vertical separation: above / center / below.
    high_x_vals = sorted({p["x"] for p in points if float(p["x"]) >= 180.0})
    for hx in high_x_vals:
        close_pts = [p for p in points if abs(float(p["x"]) - float(hx)) < 1e-9]
        close_pts_sorted = sorted(close_pts, key=lambda p: float(p["y"]))
        vertical_slots = [
            (14, "bottom"),   # above
            (0, "center"),    # center
            (-14, "top"),     # below
        ]
        if len(close_pts_sorted) == 3:
            for p, (dy, va) in zip(reversed(close_pts_sorted), vertical_slots):
                p["manual_dy"] = dy
                p["manual_va"] = va
                p["manual_dx"] = 12
                p["manual_ha"] = "left"

    for p in points:
        mode = str(p["mode"])
        x = float(p["x"])
        y = float(p["y"])
        dx, dy, ha, va = mode_offsets.get(mode, (8, 8, "left", "bottom"))

        if "manual_dx" in p:
            dx = int(p["manual_dx"])
            dy = int(p["manual_dy"])
            ha = str(p["manual_ha"])
            va = str(p["manual_va"])

        ax.annotate(
            f"{y:.1f}",
            xy=(x, y),
            xytext=(dx, dy),
            textcoords="offset points",
            ha=ha,
            va=va,
            fontsize=10.5,
            color="black",
            bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": "none", "alpha": 0.85},
            arrowprops={"arrowstyle": "-", "lw": 0.6, "color": "#666666", "alpha": 0.7},
            zorder=4,
        )

    eq_lines = [f"{mode}: y = {m:.3f}x + {b:.2f}" for mode, m, b in equations]
    eq_text = "Regression equations\n" + "\n".join(eq_lines) if eq_lines else "Regression equations unavailable"
    ax.annotate(
        eq_text,
        xy=(0.98, 0.02),
        xycoords="axes fraction",
        ha="right",
        va="bottom",
        fontsize=10.5,
        bbox={"fc": "white", "ec": COLORS["purple"], "alpha": 0.9, "boxstyle": "round,pad=0.25"},
        zorder=5,
    )

    ax.set_title("Figure 1. Detection Latency vs Heartbeat Interval")
    ax.set_xlabel("Heartbeat Interval (ms)")
    ax.set_ylabel("Median Detection Latency (ms)")
    ax.legend(title="Replication Mode", loc="upper left", framealpha=0.95)
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.7, color="#D9D9D9")
    fig.tight_layout()

    caption = (
        "Figure 1. Detection latency increases with larger heartbeat intervals for fixed failure "
        "detection. Dotted lines show linear regression fits per replication mode."
    )
    save_figure(fig, out_dir / "figure1_detection_latency_vs_hb_interval", caption)
    return caption


def figure2_scatter_detection_vs_downtime(agg: pd.DataFrame, out_dir: Path) -> str:
    """
    Figure 2: Detection latency vs downtime trade-off across configurations.
    """
    df = agg.copy()
    df = df[df["fault_type"] == "crash"]
    df = df[df["detection_latency_ms_median"].notna() & df["downtime_ms_median"].notna()]

    fig, ax = plt.subplots(figsize=(8, 5.6))

    xs = df["detection_latency_ms_median"].astype(float).to_numpy()
    ys = df["downtime_ms_median"].astype(float).to_numpy()
    labels = df["config"].astype(str).to_list()

    for i, row in df.iterrows():
        mode = row["repl_mode"]
        fd_algo = row.get("fd_algo", "fixed")
        marker = FD_MARKERS.get(fd_algo, "o")
        ax.scatter(
            row["detection_latency_ms_median"],
            row["downtime_ms_median"],
            color=MODE_COLORS.get(mode, COLORS["purple"]),
            marker=marker,
            s=90,
            edgecolors="black",
            linewidths=0.5,
        )

    # Compact numeric labels and collision-aware placement inside axes.
    annotate_points(ax, xs, ys, [f"{float(v):.1f}" for v in ys], base_offset=(6, 6))

    ax.set_title("Figure 2. Detection Latency vs Downtime Trade-off")
    ax.set_xlabel("Median Detection Latency (ms)")
    ax.set_ylabel("Median Downtime (ms)")

    # Manual legend for mode color + FD marker
    mode_handles = [
        plt.Line2D([], [], marker="o", color=MODE_COLORS[m], linestyle="None", markersize=8, label=f"mode={m}")
        for m in ["none", "sync", "async"]
        if m in set(df["repl_mode"])
    ]
    fd_handles = [
        plt.Line2D([], [], marker=FD_MARKERS[a], color="black", linestyle="None", markersize=8, label=f"fd={a}")
        for a in sorted(set(df["fd_algo"]))
    ]
    ax.legend(handles=mode_handles + fd_handles, loc="upper right", framealpha=0.95)
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.7, color="#D9D9D9")
    fig.tight_layout()

    caption = (
        "Figure 2. Configuration-level trade-off between detection latency and downtime. "
        "Color encodes replication mode and marker shape encodes failure detector algorithm."
    )
    save_figure(fig, out_dir / "figure2_detection_vs_downtime_scatter", caption)
    return caption


def figure3_heatmap_parameter_sweep(agg: pd.DataFrame, out_dir: Path) -> str:
    """
    Figure 3: heatmaps of detection latency for parameter sweep (fixed FD, crash).
    One heatmap per replication mode.
    """
    df = baseline_filter(agg)
    df = df[df["fd_algo"] == "fixed"]

    intervals = sorted(df["hb_interval_ms"].dropna().astype(int).unique().tolist())
    timeouts = sorted(df["hb_timeout_ms"].dropna().astype(int).unique().tolist())
    modes = [m for m in ["none", "sync", "async"] if m in set(df["repl_mode"])]

    if not modes:
        return "Figure 3 skipped: no data."

    cmap = LinearSegmentedColormap.from_list(
        "paper_palette",
        [COLORS["blue"], COLORS["green"], COLORS["orange"], COLORS["red"], COLORS["purple"]],
    )

    fig, axes = plt.subplots(1, len(modes), figsize=(6.2 * len(modes), 5.2), squeeze=False)
    axes = axes[0]
    vmax = df["detection_latency_ms_median"].max()
    vmin = df["detection_latency_ms_median"].min()

    for k, (ax, mode) in enumerate(zip(axes, modes)):
        matrix = np.full((len(timeouts), len(intervals)), np.nan)
        sub = df[df["repl_mode"] == mode]
        for _, row in sub.iterrows():
            i = timeouts.index(int(row["hb_timeout_ms"]))
            j = intervals.index(int(row["hb_interval_ms"]))
            matrix[i, j] = float(row["detection_latency_ms_median"])

        im = ax.imshow(matrix, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax, aspect="auto")
        ax.set_title(f"mode={mode}")
        ax.set_xlabel("Heartbeat Interval (ms)")
        if k == 0:
            ax.set_ylabel("Heartbeat Timeout (ms)")
        else:
            ax.set_ylabel("")
        ax.set_xticks(range(len(intervals)))
        ax.set_xticklabels(intervals)
        ax.set_yticks(range(len(timeouts)))
        ax.set_yticklabels(timeouts)
        ax.grid(False)

        # annotate every populated cell
        cell_fontsize = 9 if (len(intervals) * len(timeouts) > 9) else 10
        for i in range(len(timeouts)):
            for j in range(len(intervals)):
                val = matrix[i, j]
                if np.isnan(val):
                    continue
                text = f"{val:.1f}"
                color = "white" if val > (vmin + vmax) / 2 else "black"
                ax.annotate(
                    text,
                    xy=(j, i),
                    xytext=(0, 0),
                    textcoords="offset points",
                    ha="center",
                    va="center",
                    fontsize=cell_fontsize,
                    color=color,
                    bbox={"boxstyle": "round,pad=0.15", "fc": "white", "ec": "none", "alpha": 0.55}
                    if color == "black"
                    else None,
                )

    # Reserve dedicated space for colorbar to avoid overlap with right-most panel.
    fig.subplots_adjust(right=0.86)
    cbar_ax = fig.add_axes([0.89, 0.19, 0.018, 0.62])  # [left, bottom, width, height]
    cbar = fig.colorbar(im, cax=cbar_ax)
    cbar.set_label("Median Detection Latency (ms)", labelpad=10)
    cbar.ax.tick_params(labelsize=10)
    fig.suptitle("Figure 3. Parameter Sweep Heatmap of Detection Latency", fontsize=16, y=1.02)
    fig.subplots_adjust(top=0.84, wspace=0.42)

    caption = (
        "Figure 3. Heatmaps show detection latency across heartbeat interval/timeout settings "
        "for each replication mode under fixed failure detection."
    )
    save_figure(fig, out_dir / "figure3_parameter_sweep_heatmap", caption)
    return caption


def figure4_bar_metric_comparison(agg: pd.DataFrame, out_dir: Path) -> str:
    """
    Figure 4: bar charts of core metrics by replication mode.
    """
    df = baseline_filter(agg)
    df = df[df["fd_algo"] == "fixed"]
    grouped = df.groupby("repl_mode", as_index=True).agg(
        detection=("detection_latency_ms_median", "mean"),
        write=("write_latency_median_us_median", "mean"),
        throughput=("throughput_ops_sec_median", "mean"),
        downtime=("downtime_ms_median", "mean"),
    )
    grouped = grouped.reindex([m for m in ["none", "sync", "async"] if m in grouped.index])

    metrics = [
        ("detection", "Detection Latency (ms)"),
        ("downtime", "Downtime (ms)"),
        ("write", "Write Latency (us)"),
        ("throughput", "Throughput (ops/s)"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    for ax, (col, ylabel) in zip(axes, metrics):
        vals = grouped[col].to_numpy()
        modes = grouped.index.to_list()
        x = np.arange(len(modes))
        bars = ax.bar(x, vals, color=[MODE_COLORS[m] for m in modes], edgecolor="black", linewidth=0.6)
        ax.set_xticks(x)
        ax.set_xticklabels(modes)
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel)

        # annotate every bar
        for i, (b, v) in enumerate(zip(bars, vals)):
            ax.annotate(
                f"{v:.2f}",
                (b.get_x() + b.get_width() / 2, b.get_height()),
                textcoords="offset points",
                xytext=((6 if i % 2 == 0 else -6), 8),
                ha="center",
                va="bottom",
                fontsize=10.5,
                bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": "none", "alpha": 0.85},
            )
        ymax = float(np.max(vals)) if len(vals) else 1.0
        ax.set_ylim(0, ymax * 1.22)
        ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.7, color="#D9D9D9")

    fig.suptitle("Figure 4. Average Metric Comparison by Replication Mode", fontsize=16)
    fig.tight_layout()

    caption = (
        "Figure 4. Average core metrics by replication mode. Sync/async trade lower consistency risk "
        "for additional write latency while throughput remains comparable under rate-limited load."
    )
    save_figure(fig, out_dir / "figure4_bar_comparison", caption)
    return caption


def figure5_tradeoff_repl_skips(agg: pd.DataFrame, out_dir: Path) -> str:
    """
    Figure 5 (additional insight): detection latency vs replication skips.
    """
    df = agg.copy()
    df = df[df["fault_type"] == "crash"]
    df = df[df["detection_latency_ms_median"].notna() & df["repl_skipped_count_median"].notna()]

    fig, ax = plt.subplots(figsize=(8, 5.4))
    x = df["detection_latency_ms_median"].astype(float).to_numpy()
    y = df["repl_skipped_count_median"].astype(float).to_numpy()

    for _, row in df.iterrows():
        mode = row["repl_mode"]
        ax.scatter(
            row["detection_latency_ms_median"],
            row["repl_skipped_count_median"],
            color=MODE_COLORS.get(mode, COLORS["purple"]),
            marker=MODE_MARKERS.get(mode, "o"),
            s=86,
            edgecolors="black",
            linewidths=0.5,
        )

    # Compact numeric labels and collision-aware placement inside axes.
    annotate_points(ax, x, y, [f"{float(v):.1f}" for v in y], base_offset=(6, 6))

    ax.set_title("Figure 5. Detection Latency vs Replication-Skip Risk")
    ax.set_xlabel("Median Detection Latency (ms)")
    ax.set_ylabel("Median repl_skipped_count")
    ax.legend(
        handles=[
            plt.Line2D([], [], marker=MODE_MARKERS[m], color=MODE_COLORS[m], linestyle="None", markersize=8, label=m)
            for m in ["none", "sync", "async"]
            if m in set(df["repl_mode"])
        ],
        title="Replication Mode",
        loc="upper right",
    )
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.7, color="#D9D9D9")
    fig.tight_layout()

    caption = (
        "Figure 5. Additional trade-off view: configurations with more aggressive failure detection can "
        "reduce detection latency but interact with replication behavior, reflected in repl_skipped_count."
    )
    save_figure(fig, out_dir / "figure5_detection_vs_repl_skips", caption)
    return caption


def generate_all_figures(output_dir: Path) -> List[str]:
    matplotlib.use("Agg")
    plt.rcParams.update(PAPER_STYLE)

    results_dir = output_dir / "results"
    figures_dir = output_dir / "plots_paper"
    figures_dir.mkdir(parents=True, exist_ok=True)

    summary, aggregate = load_data(results_dir)
    _ = summary  # kept for future per-trial visualizations

    captions = [
        figure1_line_latency_vs_hb_interval(aggregate, figures_dir),
        figure2_scatter_detection_vs_downtime(aggregate, figures_dir),
        figure3_heatmap_parameter_sweep(aggregate, figures_dir),
        figure4_bar_metric_comparison(aggregate, figures_dir),
        figure5_tradeoff_repl_skips(aggregate, figures_dir),
    ]

    captions = [c for c in captions if c]
    with open(figures_dir / "captions_all_figures.txt", "w", encoding="utf-8") as f:
        for i, cap in enumerate(captions, start=1):
            f.write(f"Figure {i} caption: {cap}\n")
    return captions


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate paper-ready figures.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "output",
        help="Directory containing results/ and where plots_paper/ will be created.",
    )
    args = parser.parse_args()

    captions = generate_all_figures(args.output)
    print(f"Generated {len(captions)} publication-quality figures in {args.output / 'plots_paper'}")
    print("Captions saved to captions_all_figures.txt")


if __name__ == "__main__":
    main()
