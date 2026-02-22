from pathlib import Path
import subprocess
import sys

try:
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as e:
    print("Error: pandas and matplotlib are required. Run: pip install -r experiments/requirements.txt")
    raise SystemExit(1) from e

ROOT = Path(__file__).resolve().parent.parent
METRICS = ROOT / "analysis_out" / "metrics.csv"
PLOTS = ROOT / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)


def ensure_metrics():
    """Run analyze.py if metrics.csv does not exist."""
    if not METRICS.exists():
        analyze_script = ROOT / "analysis" / "analyze.py"
        subprocess.run([sys.executable, str(analyze_script)], check=True, cwd=str(ROOT))


def heatmap(df, value_col, title, out_path: Path):
    pivot = df.pivot_table(index="hb_timeout_ms", columns="hb_interval_ms", values=value_col, aggfunc="mean")
    vals = pivot.fillna(0).values
    plt.figure()
    plt.imshow(vals, aspect="auto")
    plt.xticks(range(len(pivot.columns)), pivot.columns)
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.xlabel("heartbeat_interval_ms")
    plt.ylabel("heartbeat_timeout_ms")
    plt.title(title)
    plt.colorbar()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    print("saved:", out_path)

def main():
    ensure_metrics()
    df = pd.read_csv(METRICS)
    heatmap(df, "downtime_ms", "Downtime (ms)", PLOTS / "heatmap_downtime.png")
    heatmap(df, "detection_ms", "Failure Detection Time (ms)", PLOTS / "heatmap_detection.png")

if __name__ == "__main__":
    main()