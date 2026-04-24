#!/usr/bin/env python3
"""
parse_logs.py — Parse JSONL logs from node processes and workload generators.

Reads log files from experiment trial directories and produces structured
event timelines for downstream metrics computation.

Can be used standalone for debugging or as a library imported by compute_metrics.py.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load all valid JSON lines from a JSONL file."""
    events = []
    if not path.exists():
        return events
    with open(path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"  Warning: invalid JSON at {path}:{line_num}", file=sys.stderr)
    return events


def load_trial_logs(trial_dir: Path) -> Dict[str, List[Dict]]:
    """
    Load all logs from a trial directory.

    Returns a dict with keys:
      - "node0": events from node0.jsonl
      - "node1": events from node1.jsonl
      - "workload": events from workload.jsonl
      - "injector": events from injector.jsonl
    """
    return {
        "node0": load_jsonl(trial_dir / "node0.jsonl"),
        "node1": load_jsonl(trial_dir / "node1.jsonl"),
        "workload": load_jsonl(trial_dir / "workload.jsonl"),
        "injector": load_jsonl(trial_dir / "injector.jsonl"),
    }


def filter_events(events: List[Dict], event_type: str) -> List[Dict]:
    """Filter events by event type."""
    return [e for e in events if e.get("event") == event_type]


def get_first_event(events: List[Dict], event_type: str) -> Optional[Dict]:
    """Get the first event of a given type."""
    for e in events:
        if e.get("event") == event_type:
            return e
    return None


def get_timeline(trial_dir: Path) -> List[Dict]:
    """
    Build a unified, time-sorted event timeline from all logs in a trial.
    Each event gets a 'source' field indicating which log it came from.
    """
    logs = load_trial_logs(trial_dir)
    timeline = []
    for source, events in logs.items():
        for evt in events:
            evt_copy = dict(evt)
            evt_copy["_source"] = source
            timeline.append(evt_copy)
    timeline.sort(key=lambda e: e.get("ts_ms", 0))
    return timeline


def print_timeline(trial_dir: Path):
    """Print a human-readable timeline for debugging."""
    timeline = get_timeline(trial_dir)
    for evt in timeline:
        ts = evt.get("ts_ms", "?")
        source = evt.get("_source", "?")
        event = evt.get("event", "?")
        extra_parts = []
        for k in ["key", "op_type", "peer_id", "fault_type", "detection_latency_ms",
                   "latency_us", "success", "delay_ms"]:
            if k in evt:
                extra_parts.append(f"{k}={evt[k]}")
            elif "extra" in evt and isinstance(evt["extra"], dict) and k in evt["extra"]:
                extra_parts.append(f"{k}={evt['extra'][k]}")
        extra_str = " " + " ".join(extra_parts) if extra_parts else ""
        print(f"[{ts}] {source:>10} | {event:<25}{extra_str}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Parse and display experiment logs")
    parser.add_argument("trial_dir", type=Path, help="Path to a trial directory")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    if not args.trial_dir.is_dir():
        print(f"Error: {args.trial_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    if args.json:
        timeline = get_timeline(args.trial_dir)
        print(json.dumps(timeline, indent=2))
    else:
        print_timeline(args.trial_dir)


if __name__ == "__main__":
    main()
