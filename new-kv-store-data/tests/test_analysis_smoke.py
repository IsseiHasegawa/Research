"""Smoke tests for log analysis pipeline (no C++ build required)."""

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class TestAnalysisSmoke(unittest.TestCase):
    def test_compute_metrics_exits_zero(self) -> None:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "compute_metrics.py"), "--output", str(ROOT / "output")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr or proc.stdout)

    def test_summary_csv_exists_after_metrics(self) -> None:
        summary = ROOT / "output" / "results" / "summary.csv"
        self.assertTrue(summary.is_file(), "Run compute_metrics first or commit output/logs")


if __name__ == "__main__":
    unittest.main()
