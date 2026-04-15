#!/usr/bin/env python3
"""
tests/test_implementation.py — Implementation correctness verification.

Tests specifically target implementation details and known edge cases:

  1. Version vector node_id correctness
       - open_wal() must set node_id_ even when WAL is disabled
       - Version key must be the node's own --id, not the fallback "node"
  2. WAL version consistency
       - WAL entries and in-memory GET must agree on version vectors
       - Overwrites produce strictly increasing sequence numbers
       - Crash + WAL recovery restores exact version vectors
  3. Wire protocol compliance
       - Required fields in KV_SET_RESP / KV_GET_RESP / FAULT_DELAY_ACK
       - req_id echo, null value for missing keys, empty version for missing keys
  4. Replication version propagation
       - Sync replication must deliver exact version to secondary
       - repl_skipped must appear after peer declared dead
  5. Concurrent write safety
       - Concurrent writers must get unique, non-duplicate sequence numbers
       - All concurrent writes must be readable after completion
  6. Fault delay behavior
       - FAULT_DELAY must slow down both SET and GET
       - delay_ms=0 must cancel the delay immediately
       - fault_injected events must appear in the log
  7. Log event correctness
       - client_req_recv / client_req_done must be 1-1 matched by req_id
       - node_start must include config fields
       - ts_ms must be positive and recent
       - wal_recovered event must appear on restart with existing WAL

Usage:
  cd new-kv-store
  python3 tests/test_implementation.py          # all tests
  python3 tests/test_implementation.py -v       # verbose
  python3 tests/test_implementation.py TestVersionVectorNodeId   # one class
"""

import json
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).resolve().parent.parent
KVNODE   = ROOT / "build" / "kvnode"
WORKLOAD = ROOT / "build" / "kv_workload"

# Use a high port offset to avoid collision with test_kvstore.py (19100–19200)
# and test_all.py (19100–~19200).  This file starts at port 19700.
_port_counter = 300


def next_ports():
    """Return two unique ports for a test."""
    global _port_counter
    base = 19100 + _port_counter * 2
    _port_counter += 1
    return base, base + 1


# ─── Helpers ──────────────────────────────────────────────────────────────────

_COMPACT = (',', ':')  # compact JSON separators required for C++ parser compatibility


def wait_port(port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def send_recv(port: int, msg: str, timeout: float = 5.0) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(msg.encode())
        s.settimeout(timeout)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        return buf.decode().strip()


def parse_jsonl(path: Path) -> list:
    events = []
    if not path.exists():
        return events
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events


def make_set(key: str, value: str, req_id: str) -> str:
    # Use compact separators: Python json.dumps default inserts spaces after ':' and ','
    # but the C++ extract_type / extract_string parsers look for exact strings without spaces.
    return json.dumps({"type": "KV_SET", "key": key, "value": value,
                       "req_id": req_id, "version": {}},
                      separators=_COMPACT) + "\n"


def make_get(key: str, req_id: str) -> str:
    return json.dumps({"type": "KV_GET", "key": key, "req_id": req_id},
                      separators=_COMPACT) + "\n"


def make_fault_delay(delay_ms: int) -> str:
    return json.dumps({"type": "FAULT_DELAY", "delay_ms": delay_ms},
                      separators=_COMPACT) + "\n"


class NodeProcess:
    """Context manager that starts / stops a kvnode process."""

    def __init__(self, node_id, port, log_path, wal_path=None, run_id="impl_test",
                 is_primary=False, peer_port=None, repl_mode="none",
                 fd_algo="fixed", hb_interval_ms=50, hb_timeout_ms=150):
        self.node_id        = node_id
        self.port           = port
        self.log_path       = log_path
        self.wal_path       = wal_path
        self.run_id         = run_id
        self.is_primary     = is_primary
        self.peer_port      = peer_port
        self.repl_mode      = repl_mode
        self.fd_algo        = fd_algo
        self.hb_interval_ms = hb_interval_ms
        self.hb_timeout_ms  = hb_timeout_ms
        self.proc           = None

    def start(self):
        cmd = [str(KVNODE),
               "--id",              self.node_id,
               "--port",            str(self.port),
               "--log_path",        str(self.log_path),
               "--run_id",          self.run_id,
               "--hb_interval_ms",  str(self.hb_interval_ms),
               "--hb_timeout_ms",   str(self.hb_timeout_ms),
               "--repl_mode",       self.repl_mode,
               "--fd_algo",         self.fd_algo]
        if self.wal_path:
            cmd += ["--wal_path", str(self.wal_path)]
        if self.is_primary:
            cmd.append("--primary")
            if self.peer_port:
                cmd += ["--peer", f"127.0.0.1:{self.peer_port}"]
        self.proc = subprocess.Popen(cmd,
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        assert wait_port(self.port, timeout=5.0), \
            f"Node {self.node_id} failed to start on port {self.port}"
        time.sleep(0.1)
        return self

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def kill(self):
        """Simulate crash (no graceful shutdown)."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


def skip_if_no_binary(func):
    """Skip a test method when the kvnode binary has not been built yet."""
    import functools
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not KVNODE.exists():
            self.skipTest(
                f"kvnode binary not found at {KVNODE}. "
                f"Build with: cd {ROOT} && mkdir -p build && cd build && cmake .. && make"
            )
        return func(self, *args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Version vector node_id correctness
# ═══════════════════════════════════════════════════════════════════════════════

class TestVersionVectorNodeId(unittest.TestCase):
    """
    Version vectors must use the node's own --id as the dictionary key.

    Root cause of the original bug:
      kv_store.hpp::open_wal() had an early return before setting node_id_,
      so when WAL was disabled (the default in most configs), node_id_ stayed
      empty and set_local() fell back to the literal string "node".

    Fix: node_id_ is now set unconditionally at the start of open_wal(), and
    node.hpp always calls open_wal() (even with an empty path) so the store
    is initialized regardless of WAL configuration.

    Note: version tracking only applies to primary nodes (set_local is called
    only in the primary code path).  Tests use is_primary=True without a peer,
    which is allowed after relaxing the main.cpp validation.
    """

    @skip_if_no_binary
    def test_version_key_matches_node_id_without_wal(self):
        """
        Without --wal_path: the version key in client_req_done must be the
        node's --id, not the fallback literal "node".
        """
        p0, _ = next_ports()
        node_id = "mynode"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess(node_id, p0, log0, is_primary=True):  # no wal_path
                send_recv(p0, make_set("k1", "v1", "r1"))
                time.sleep(0.1)

            events = parse_jsonl(log0)
            done = next(
                (e for e in events
                 if e.get("event") == "client_req_done"
                 and e.get("extra", {}).get("op") == "SET"),
                None,
            )
            self.assertIsNotNone(done, "client_req_done SET event not found in log")
            version = done.get("extra", {}).get("version", {})
            self.assertIn(node_id, version,
                          f"Version key should be '{node_id}', got keys: {list(version.keys())}")
            self.assertNotIn("node", version,
                             "Version must not use the fallback key 'node'")

    @skip_if_no_binary
    def test_version_key_matches_node_id_with_wal(self):
        """
        With --wal_path: both the WAL entry and the log event must use the
        node's own ID as the version key.
        """
        p0, _ = next_ports()
        node_id = "walnode"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            wal0 = Path(tmp) / "n.wal"
            with NodeProcess(node_id, p0, log0, wal_path=wal0, is_primary=True):
                send_recv(p0, make_set("k1", "v1", "r1"))
                time.sleep(0.1)

            # WAL entry
            wal_entries = parse_jsonl(wal0)
            self.assertEqual(len(wal_entries), 1, "Expected exactly 1 WAL entry")
            wal_version = wal_entries[0].get("version", {})
            self.assertIn(node_id, wal_version,
                          f"WAL version key should be '{node_id}', "
                          f"got: {list(wal_version.keys())}")
            self.assertNotIn("node", wal_version)

            # Log event
            events = parse_jsonl(log0)
            done = next(
                (e for e in events
                 if e.get("event") == "client_req_done"
                 and e.get("extra", {}).get("op") == "SET"),
                None,
            )
            self.assertIsNotNone(done)
            log_version = done.get("extra", {}).get("version", {})
            self.assertIn(node_id, log_version,
                          f"Log version key should be '{node_id}', "
                          f"got: {list(log_version.keys())}")

    @skip_if_no_binary
    def test_version_sequence_increases_per_write(self):
        """
        Each successive write on the same node must increment the local
        sequence number.  The N-th write must have sequence N (1-indexed).
        """
        p0, _ = next_ports()
        node_id = "seqnode"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess(node_id, p0, log0, is_primary=True):
                for i in range(5):
                    send_recv(p0, make_set(f"key{i}", f"val{i}", f"r{i}"))
                time.sleep(0.1)

            events = parse_jsonl(log0)
            done_events = [
                e for e in events
                if e.get("event") == "client_req_done"
                and e.get("extra", {}).get("op") == "SET"
            ]
            self.assertEqual(len(done_events), 5, "Expected 5 SET done events")

            seqs = [e["extra"]["version"].get(node_id, 0) for e in done_events]
            self.assertEqual(seqs, list(range(1, 6)),
                             f"Version sequences must be [1,2,3,4,5], got {seqs}")

    @skip_if_no_binary
    def test_get_returns_version_with_correct_node_id(self):
        """
        GET response version must use the node's own ID as key and the
        sequence number must match what was assigned at write time.
        """
        p0, _ = next_ports()
        node_id = "getver"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess(node_id, p0, log0, is_primary=True):
                send_recv(p0, make_set("mykey", "myval", "w1"))
                resp = json.loads(send_recv(p0, make_get("mykey", "g1")))

            self.assertTrue(resp.get("ok"), "GET should succeed")
            self.assertIn("version", resp, "GET response must include 'version'")
            version = resp["version"]
            self.assertIsInstance(version, dict)
            self.assertIn(node_id, version,
                          f"GET version key should be '{node_id}', "
                          f"got: {list(version.keys())}")
            self.assertEqual(version[node_id], 1,
                             f"First write → seq=1 expected, got version={version}")

    @skip_if_no_binary
    def test_get_missing_key_returns_empty_version(self):
        """GET for a non-existent key must return version={} (not omit the field)."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0):
                resp = json.loads(send_recv(p0, make_get("no_such_key", "g1")))

            self.assertFalse(resp.get("ok"))
            self.assertIsNone(resp.get("value"), "Missing key value must be null")
            self.assertIn("version", resp, "GET response must always contain 'version'")
            self.assertEqual(resp["version"], {},
                             f"Missing key version must be {{}}, "
                             f"got {resp.get('version')}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. WAL version consistency
# ═══════════════════════════════════════════════════════════════════════════════

class TestWALVersionConsistency(unittest.TestCase):
    """WAL entries and in-memory GET must agree on version vectors."""

    @skip_if_no_binary
    def test_wal_version_matches_get_response(self):
        """
        The version stored in the WAL for a key must equal the version
        returned by a subsequent GET for the same key.
        """
        p0, _ = next_ports()
        node_id = "walver"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            wal0 = Path(tmp) / "n.wal"
            with NodeProcess(node_id, p0, log0, wal_path=wal0, is_primary=True):
                send_recv(p0, make_set("wk", "wv", "w1"))
                resp = json.loads(send_recv(p0, make_get("wk", "g1")))
                time.sleep(0.1)

            wal_entries = parse_jsonl(wal0)
            self.assertEqual(len(wal_entries), 1, "Expected 1 WAL entry")
            wal_version = wal_entries[0]["version"]
            get_version = resp["version"]
            self.assertEqual(wal_version, get_version,
                             f"WAL version {wal_version} != GET version {get_version}")

    @skip_if_no_binary
    def test_wal_overwrites_have_strictly_increasing_sequences(self):
        """
        Multiple SETs to the same key must each produce a WAL entry with
        strictly increasing sequence numbers — not equal, not decreasing.
        """
        p0, _ = next_ports()
        node_id = "walseq"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            wal0 = Path(tmp) / "n.wal"
            with NodeProcess(node_id, p0, log0, wal_path=wal0, is_primary=True):
                for i in range(4):
                    send_recv(p0, make_set("samekey", f"val{i}", f"w{i}"))
                time.sleep(0.1)

            entries = parse_jsonl(wal0)
            self.assertEqual(len(entries), 4, "Expected 4 WAL entries (one per SET)")
            seqs = [e["version"].get(node_id, 0) for e in entries]
            for i in range(1, len(seqs)):
                self.assertGreater(seqs[i], seqs[i - 1],
                                   f"WAL seq[{i}]={seqs[i]} must be > "
                                   f"seq[{i-1}]={seqs[i-1]}")

    @skip_if_no_binary
    def test_recovery_restores_exact_version_vectors(self):
        """
        After crash + WAL restart, GET must return the exact same version
        that was stored before the crash — not a default or empty version.
        """
        p0, _ = next_ports()
        node_id = "recver"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            wal0 = Path(tmp) / "n.wal"

            # Phase 1: write 3 keys, record versions, then crash
            n0 = NodeProcess(node_id, p0, log0, wal_path=wal0,
                              run_id="p1", is_primary=True)
            n0.start()
            original_versions = {}
            for i in range(3):
                send_recv(p0, make_set(f"rk{i}", f"rv{i}", f"w{i}"))
                get_resp = json.loads(send_recv(p0, make_get(f"rk{i}", f"g{i}")))
                original_versions[f"rk{i}"] = get_resp["version"]
            time.sleep(0.1)
            n0.kill()
            time.sleep(0.2)

            # Phase 2: restart with same WAL, check versions are identical
            p0b, _ = next_ports()
            log0b = Path(tmp) / "nb.jsonl"
            with NodeProcess(node_id, p0b, log0b, wal_path=wal0,
                             run_id="p2", is_primary=True):
                for i in range(3):
                    get_resp = json.loads(send_recv(p0b, make_get(f"rk{i}", f"gr{i}")))
                    self.assertTrue(get_resp.get("ok"),
                                    f"Key rk{i} not found after WAL recovery")
                    self.assertEqual(get_resp.get("value"), f"rv{i}",
                                     f"rk{i} value mismatch after recovery")
                    recovered_ver = get_resp["version"]
                    self.assertEqual(
                        recovered_ver, original_versions[f"rk{i}"],
                        f"rk{i}: original={original_versions[f'rk{i}']}, "
                        f"recovered={recovered_ver}"
                    )


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Wire protocol compliance
# ═══════════════════════════════════════════════════════════════════════════════

class TestMessageProtocol(unittest.TestCase):
    """Verify wire message fields comply with message.hpp specification."""

    @skip_if_no_binary
    def test_kv_set_resp_required_fields(self):
        """KV_SET_RESP must contain: type, key, ok, req_id.  Must NOT contain value."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                resp = json.loads(send_recv(p0, make_set("k", "v", "myreqid")))

        self.assertEqual(resp.get("type"), "KV_SET_RESP")
        self.assertEqual(resp.get("key"), "k")
        self.assertTrue(resp.get("ok"))
        self.assertEqual(resp.get("req_id"), "myreqid")
        # SET response is an ack — it must NOT echo back the written value
        self.assertNotIn("value", resp,
                         "KV_SET_RESP must not contain a 'value' field")

    @skip_if_no_binary
    def test_kv_get_resp_fields_when_found(self):
        """KV_GET_RESP for an existing key: type, key, value, ok=true, version, req_id."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                send_recv(p0, make_set("mykey", "myval", "w1"))
                resp = json.loads(send_recv(p0, make_get("mykey", "g1")))

        self.assertEqual(resp.get("type"), "KV_GET_RESP")
        self.assertEqual(resp.get("key"), "mykey")
        self.assertEqual(resp.get("value"), "myval")
        self.assertTrue(resp.get("ok"))
        self.assertEqual(resp.get("req_id"), "g1")
        self.assertIn("version", resp, "KV_GET_RESP must include 'version'")
        self.assertIsInstance(resp["version"], dict)

    @skip_if_no_binary
    def test_kv_get_resp_fields_when_missing(self):
        """KV_GET_RESP for a missing key: ok=false, value=null, version={}."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                resp = json.loads(send_recv(p0, make_get("nosuchkey", "g1")))

        self.assertEqual(resp.get("type"), "KV_GET_RESP")
        self.assertFalse(resp.get("ok"))
        self.assertIsNone(resp.get("value"),
                          "value must be null (None) for a missing key")
        self.assertIn("version", resp,
                      "KV_GET_RESP must include 'version' even when key is missing")
        self.assertEqual(resp["version"], {},
                         f"Missing key version must be {{}}, got {resp.get('version')}")

    @skip_if_no_binary
    def test_fault_delay_ack_fields(self):
        """FAULT_DELAY_ACK must contain: type=FAULT_DELAY_ACK, ok=true."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                resp = json.loads(send_recv(p0, make_fault_delay(0)))

        self.assertEqual(resp.get("type"), "FAULT_DELAY_ACK")
        self.assertTrue(resp.get("ok"))

    @skip_if_no_binary
    def test_req_id_is_echoed_in_both_responses(self):
        """Both SET and GET responses must echo back the exact req_id."""
        p0, _ = next_ports()
        set_id = "set-id-unique-abc"
        get_id = "get-id-unique-xyz"
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                set_resp = json.loads(send_recv(p0, make_set("k", "v", set_id)))
                get_resp = json.loads(send_recv(p0, make_get("k", get_id)))

        self.assertEqual(set_resp.get("req_id"), set_id,
                         "SET response must echo req_id")
        self.assertEqual(get_resp.get("req_id"), get_id,
                         "GET response must echo req_id")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Replication version propagation
# ═══════════════════════════════════════════════════════════════════════════════

class TestReplicationVersionPropagation(unittest.TestCase):
    """Version vectors must propagate correctly through sync replication."""

    @skip_if_no_binary
    def test_sync_secondary_has_identical_version_as_primary(self):
        """
        After sync replication, GET on secondary must return the exact same
        version dict as GET on primary for the same key.
        """
        p0, p1 = next_ports()
        node_id = "prim"
        with tempfile.TemporaryDirectory() as tmp:
            n1 = NodeProcess("sec", p1, Path(tmp) / "n1.jsonl")
            n0 = NodeProcess(node_id, p0, Path(tmp) / "n0.jsonl",
                             is_primary=True, peer_port=p1, repl_mode="sync")
            with n1, n0:
                time.sleep(0.3)
                send_recv(p0, make_set("repkey", "repval", "w1"))
                time.sleep(0.2)
                p_resp = json.loads(send_recv(p0, make_get("repkey", "gp")))
                s_resp = json.loads(send_recv(p1, make_get("repkey", "gs")))

        self.assertTrue(p_resp.get("ok"), "Primary GET must succeed")
        self.assertTrue(s_resp.get("ok"), "Secondary GET must succeed — key not replicated")
        self.assertEqual(p_resp.get("value"), s_resp.get("value"),
                         "Primary and secondary values must match")
        self.assertEqual(p_resp.get("version"), s_resp.get("version"),
                         f"Version mismatch: primary={p_resp.get('version')}, "
                         f"secondary={s_resp.get('version')}")

    @skip_if_no_binary
    def test_sync_repl_all_keys_version_consistent(self):
        """All keys written via sync replication must have matching versions on both nodes."""
        p0, p1 = next_ports()
        node_id = "pnode"
        with tempfile.TemporaryDirectory() as tmp:
            n1 = NodeProcess("snode", p1, Path(tmp) / "n1.jsonl")
            n0 = NodeProcess(node_id, p0, Path(tmp) / "n0.jsonl",
                             is_primary=True, peer_port=p1, repl_mode="sync")
            with n1, n0:
                time.sleep(0.3)
                for i in range(5):
                    send_recv(p0, make_set(f"k{i}", f"v{i}", f"w{i}"))
                time.sleep(0.3)

                for i in range(5):
                    pv = json.loads(send_recv(p0, make_get(f"k{i}", f"gp{i}")))
                    sv = json.loads(send_recv(p1, make_get(f"k{i}", f"gs{i}")))
                    self.assertTrue(sv.get("ok"), f"k{i} missing on secondary")
                    self.assertEqual(pv["version"], sv["version"],
                                     f"k{i}: primary version {pv['version']} "
                                     f"!= secondary version {sv['version']}")

    @skip_if_no_binary
    def test_repl_skipped_logged_after_peer_declared_dead(self):
        """
        After the secondary is killed and declared dead, writes on the primary
        must generate repl_skipped events (replication cannot proceed).
        """
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp) / "n1.jsonl")
            n0 = NodeProcess("n0", p0, log0,
                             is_primary=True, peer_port=p1, repl_mode="async",
                             hb_interval_ms=50, hb_timeout_ms=150)
            n1.start()
            n0.start()
            time.sleep(2.5)  # wait past grace period (2 000 ms)

            n1.kill()
            time.sleep(1.5)  # wait for detection

            send_recv(p0, make_set("after_dead", "v", "w1"))
            time.sleep(0.2)
            n0.stop()

            events = parse_jsonl(log0)

        dead_events = [e for e in events if e.get("event") == "declared_dead"]
        self.assertGreater(len(dead_events), 0,
                           "Precondition: declared_dead must exist before checking repl_skipped")

        skipped = [e for e in events if e.get("event") == "repl_skipped"]
        self.assertGreater(len(skipped), 0,
                           "repl_skipped must be logged when replicating after peer is dead")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Concurrent write safety
# ═══════════════════════════════════════════════════════════════════════════════

class TestConcurrentWriteSafety(unittest.TestCase):
    """Version counter (local_seq_) must be thread-safe under concurrent writes."""

    @skip_if_no_binary
    def test_concurrent_writers_get_unique_sequences(self):
        """
        N concurrent clients each writing distinct keys must receive unique
        sequence numbers — no two writes may share the same version seq.
        """
        p0, _ = next_ports()
        node_id = "concnode"
        num_clients = 5
        writes_per_client = 10
        error_flag = threading.Event()

        def writer(client_idx):
            try:
                for j in range(writes_per_client):
                    send_recv(p0, make_set(f"c{client_idx}_k{j}", "v",
                                          f"c{client_idx}_r{j}"))
            except Exception:
                error_flag.set()

        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess(node_id, p0, log0, is_primary=True):
                threads = [threading.Thread(target=writer, args=(i,))
                           for i in range(num_clients)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10)
                time.sleep(0.2)

            self.assertFalse(error_flag.is_set(), "A writer thread raised an exception")

            events = parse_jsonl(log0)

        done_events = [
            e for e in events
            if e.get("event") == "client_req_done"
            and e.get("extra", {}).get("op") == "SET"
        ]
        expected = num_clients * writes_per_client
        self.assertEqual(len(done_events), expected,
                         f"Expected {expected} SET done events, got {len(done_events)}")

        seqs = [e["extra"]["version"].get(node_id, 0) for e in done_events]
        self.assertEqual(len(seqs), len(set(seqs)),
                         f"Duplicate sequence numbers detected: {sorted(seqs)}")

    @skip_if_no_binary
    def test_concurrent_writes_all_readable_after_completion(self):
        """All values written concurrently must be readable with correct values."""
        p0, _ = next_ports()
        num_keys = 20
        results = {}
        lock = threading.Lock()

        def write_key(key, value):
            resp = json.loads(send_recv(p0, make_set(key, value, f"w_{key}")))
            with lock:
                results[key] = resp.get("ok", False)

        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0):
                threads = [
                    threading.Thread(target=write_key,
                                     args=(f"key_{i}", f"val_{i}"))
                    for i in range(num_keys)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10)

                for i in range(num_keys):
                    resp = json.loads(send_recv(p0, make_get(f"key_{i}", f"g_{i}")))
                    self.assertTrue(resp.get("ok"),
                                    f"key_{i} not found after concurrent writes")
                    self.assertEqual(resp.get("value"), f"val_{i}",
                                     f"key_{i}: expected val_{i}, got {resp.get('value')}")

        for key, ok in results.items():
            self.assertTrue(ok, f"{key}: SET returned ok=false")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Fault delay behavior
# ═══════════════════════════════════════════════════════════════════════════════

class TestFaultDelayBehavior(unittest.TestCase):
    """FAULT_DELAY must affect all subsequent operations and be cancellable."""

    @skip_if_no_binary
    def test_delay_slows_down_get(self):
        """FAULT_DELAY must increase GET latency by at least the requested delay."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                send_recv(p0, make_set("k", "v", "w1"))

                # Inject 200 ms delay
                send_recv(p0, make_fault_delay(200))

                t0 = time.time()
                resp = json.loads(send_recv(p0, make_get("k", "g1"), timeout=5.0))
                elapsed_ms = (time.time() - t0) * 1000

                # Cancel delay
                send_recv(p0, make_fault_delay(0))

        self.assertTrue(resp.get("ok"))
        self.assertGreater(elapsed_ms, 150,
                           f"GET should be delayed ~200 ms, got {elapsed_ms:.0f} ms")

    @skip_if_no_binary
    def test_delay_slows_down_set(self):
        """FAULT_DELAY must also increase SET latency."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                send_recv(p0, make_fault_delay(200))

                t0 = time.time()
                resp = json.loads(send_recv(p0, make_set("k", "v", "w1"), timeout=5.0))
                elapsed_ms = (time.time() - t0) * 1000

                send_recv(p0, make_fault_delay(0))

        self.assertTrue(resp.get("ok"))
        self.assertGreater(elapsed_ms, 150,
                           f"SET should be delayed ~200 ms, got {elapsed_ms:.0f} ms")

    @skip_if_no_binary
    def test_delay_is_cancellable(self):
        """Setting delay_ms=0 must cancel the delay for all subsequent operations."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            with NodeProcess("n0", p0, Path(tmp) / "n.jsonl"):
                # Inject, then immediately cancel
                send_recv(p0, make_fault_delay(500))
                send_recv(p0, make_fault_delay(0))

                t0 = time.time()
                send_recv(p0, make_set("k", "v", "w1"))
                elapsed_ms = (time.time() - t0) * 1000

        self.assertLess(elapsed_ms, 200,
                        f"After cancelling delay, op must be fast. Got {elapsed_ms:.0f} ms")

    @skip_if_no_binary
    def test_delay_produces_fault_injected_log_events(self):
        """FAULT_DELAY must produce fault_injected events with correct delay_ms."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0):
                send_recv(p0, make_fault_delay(75))
                time.sleep(0.1)
                send_recv(p0, make_fault_delay(0))
                time.sleep(0.1)

            events = parse_jsonl(log0)

        fault_events = [e for e in events if e.get("event") == "fault_injected"]
        self.assertGreaterEqual(len(fault_events), 2,
                                "Expected ≥2 fault_injected events (inject + cancel)")
        delays = [e.get("extra", {}).get("delay_ms") for e in fault_events]
        self.assertIn(75, delays, "fault_injected must record delay_ms=75")
        self.assertIn(0, delays, "fault_injected must record delay_ms=0 (cancel)")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Log event correctness
# ═══════════════════════════════════════════════════════════════════════════════

class TestLogEventCorrectness(unittest.TestCase):
    """Log events must be structurally correct and semantically coherent."""

    @skip_if_no_binary
    def test_client_req_recv_and_done_are_one_to_one_by_req_id(self):
        """
        Every client_req_recv must be paired with exactly one client_req_done
        that carries the same req_id (no orphan receives or dangling completions).
        """
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0):
                for i in range(5):
                    send_recv(p0, make_set(f"k{i}", f"v{i}", f"set_{i}"))
                    send_recv(p0, make_get(f"k{i}", f"get_{i}"))
                time.sleep(0.1)

            events = parse_jsonl(log0)

        recv_ids = {e["extra"]["req_id"]
                    for e in events
                    if e.get("event") == "client_req_recv"
                    and "req_id" in e.get("extra", {})}
        done_ids = {e["extra"]["req_id"]
                    for e in events
                    if e.get("event") == "client_req_done"
                    and "req_id" in e.get("extra", {})}
        self.assertGreater(len(recv_ids), 0, "Expected at least one client event")
        self.assertEqual(recv_ids, done_ids,
                         f"Unmatched req_ids: recv-only={recv_ids - done_ids}, "
                         f"done-only={done_ids - recv_ids}")

    @skip_if_no_binary
    def test_node_start_event_contains_config_fields(self):
        """node_start extra must include: port, repl_mode, fd_algo, is_primary."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0, run_id="cfgtest",
                             repl_mode="none", fd_algo="fixed"):
                time.sleep(0.1)

            events = parse_jsonl(log0)

        start = next((e for e in events if e.get("event") == "node_start"), None)
        self.assertIsNotNone(start, "node_start event not found")
        extra = start.get("extra", {})
        for field in ["port", "repl_mode", "fd_algo", "is_primary"]:
            self.assertIn(field, extra,
                          f"node_start extra missing required field '{field}'")
        self.assertEqual(extra["port"], p0)
        self.assertEqual(extra["repl_mode"], "none")
        self.assertEqual(extra["fd_algo"], "fixed")
        self.assertFalse(extra["is_primary"])

    @skip_if_no_binary
    def test_all_log_ts_ms_are_positive_and_recent(self):
        """All log events must have ts_ms > 0 and within 60 s of the current time."""
        p0, _ = next_ports()
        now_ms = int(time.time() * 1000)
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0):
                send_recv(p0, make_set("k", "v", "r1"))
                time.sleep(0.1)

            events = parse_jsonl(log0)

        self.assertGreater(len(events), 0, "Log must contain at least one event")
        for e in events:
            ts = e.get("ts_ms", 0)
            self.assertGreater(ts, 0, f"ts_ms must be positive: {e}")
            self.assertLess(abs(ts - now_ms), 60_000,
                            f"ts_ms {ts} is more than 60 s away from now={now_ms}: {e}")

    @skip_if_no_binary
    def test_wal_recovered_event_logged_on_restart(self):
        """
        After a crash + WAL-enabled restart, the new run must log a
        wal_recovered event reporting the correct number of recovered entries.
        """
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            wal0 = Path(tmp) / "n.wal"

            # Phase 1: write 3 keys, then crash
            n0 = NodeProcess("n0", p0, log0, wal_path=wal0,
                             run_id="p1", is_primary=True)
            n0.start()
            for i in range(3):
                send_recv(p0, make_set(f"k{i}", f"v{i}", f"w{i}"))
            time.sleep(0.1)
            n0.kill()
            time.sleep(0.2)

            # Phase 2: restart
            p0b, _ = next_ports()
            log0b = Path(tmp) / "nb.jsonl"
            with NodeProcess("n0", p0b, log0b, wal_path=wal0,
                             run_id="p2", is_primary=True):
                time.sleep(0.1)

            events = parse_jsonl(log0b)

        recovered = next((e for e in events
                          if e.get("event") == "wal_recovered"), None)
        self.assertIsNotNone(recovered,
                             "wal_recovered event not found in restart log")
        entries = recovered.get("extra", {}).get("entries", -1)
        self.assertEqual(entries, 3,
                         f"wal_recovered must report 3 entries, got {entries}")

    @skip_if_no_binary
    def test_log_run_id_matches_configured_value(self):
        """Every log event must carry the run_id that was passed via --run_id."""
        p0, _ = next_ports()
        run_id = "my-experiment-run-42"
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n.jsonl"
            with NodeProcess("n0", p0, log0, run_id=run_id):
                send_recv(p0, make_set("k", "v", "r1"))
                time.sleep(0.1)

            events = parse_jsonl(log0)

        self.assertGreater(len(events), 0)
        for e in events:
            self.assertEqual(e.get("run_id"), run_id,
                             f"run_id mismatch in event: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

TEST_CLASSES = [
    TestVersionVectorNodeId,
    TestWALVersionConsistency,
    TestMessageProtocol,
    TestReplicationVersionPropagation,
    TestConcurrentWriteSafety,
    TestFaultDelayBehavior,
    TestLogEventCorrectness,
]


def main():
    print("=" * 65)
    print("KV Store — Implementation Correctness Tests")
    print("=" * 65)

    if not KVNODE.exists():
        print(
            f"\nERROR: Build first:\n"
            f"  cd {ROOT} && mkdir -p build && cd build && cmake .. && make\n"
        )
        sys.exit(1)

    # Allow running a single class by name
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        target = sys.argv[1]
        matched = [c for c in TEST_CLASSES if c.__name__ == target]
        if not matched:
            print(f"Unknown class '{target}'.")
            print(f"Available: {[c.__name__ for c in TEST_CLASSES]}")
            sys.exit(1)
        classes = matched
    else:
        classes = TEST_CLASSES

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
