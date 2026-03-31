#!/usr/bin/env python3
"""
test_kvstore.py — Integration tests for the distributed KV store.

Validates:
  1. Basic KV operations (SET / GET)
  2. GET for non-existent key returns ok=false
  3. SET overwrite semantics
  4. Heartbeat exchange (ping/ack)
  5. Failure detection (kill secondary → declared_dead)
  6. Detection latency is within expected bounds
  7. Synchronous replication (SET on primary → visible on secondary)
  8. Asynchronous replication (SET on primary → eventually visible on secondary)
  9. No-replication mode (SET on primary → NOT visible on secondary)
 10. Replication stops after peer declared dead (repl_skipped)
 11. Workload generator produces valid logs
 12. Log format correctness (JSONL, required fields)
 13. Fault delay injection via control message
 14. Multiple concurrent clients

Usage:
  cd new-kv-store
  python3 tests/test_kvstore.py          # run all tests
  python3 tests/test_kvstore.py -v       # verbose
  python3 tests/test_kvstore.py -k test_basic_set_get   # run one test
"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

# ─── Paths ─────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
KVNODE = ROOT / "build" / "kvnode"
KV_WORKLOAD = ROOT / "build" / "kv_workload"

# Base ports for tests — each test class uses an offset to avoid collisions
BASE_PORT = 19100


def wall_ms() -> int:
    return int(time.time() * 1000)


# ─── TCP helpers ───────────────────────────────────────────────────────────────

def tcp_send_recv(host: str, port: int, msg: str, timeout: float = 3.0) -> str:
    """Send a JSON message and receive one line response."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    s.sendall((msg.strip() + "\n").encode())
    buf = b""
    while b"\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    s.close()
    return buf.decode().strip()


def tcp_send_recv_multi(host: str, port: int, msgs: list, timeout: float = 3.0) -> list:
    """Send multiple JSON messages on one connection and receive responses."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    responses = []
    for msg in msgs:
        s.sendall((msg.strip() + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        responses.append(buf.decode().strip())
    s.close()
    return responses


def wait_for_port(port: int, timeout: float = 5.0) -> bool:
    """Wait until a TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    return False


def kill_proc(proc, use_sigkill=False):
    """Kill a subprocess reliably."""
    if proc.poll() is not None:
        return
    try:
        if use_sigkill:
            proc.send_signal(signal.SIGKILL)
        else:
            proc.terminate()
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    except Exception:
        pass


def load_jsonl(path: Path) -> list:
    """Load a JSONL file as a list of dicts."""
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


# ─── Test base class ───────────────────────────────────────────────────────────

class KVTestBase(unittest.TestCase):
    """Base class that manages node processes and temp directories."""

    PORT_OFFSET = 0  # Override in subclasses to avoid port collisions

    @classmethod
    def setUpClass(cls):
        if not KVNODE.exists():
            raise unittest.SkipTest(
                f"Binary not found: {KVNODE}. "
                f"Build first: cd {ROOT} && mkdir -p build && cd build && cmake .. && make"
            )

    def setUp(self):
        self.procs = []
        self.tmpdir = Path(tempfile.mkdtemp(prefix="kvtest_"))
        self.port0 = BASE_PORT + self.__class__.PORT_OFFSET
        self.port1 = BASE_PORT + self.__class__.PORT_OFFSET + 1

    def tearDown(self):
        for proc in self.procs:
            kill_proc(proc)
        # Brief pause for port cleanup
        time.sleep(0.3)

    def start_node(self, node_id, port, primary=False, peer_addr=None,
                   repl_mode="none", hb_interval=100, hb_timeout=400,
                   run_id="test") -> subprocess.Popen:
        """Start a kvnode process and wait for it to be ready."""
        log_path = self.tmpdir / f"{node_id}.jsonl"
        cmd = [
            str(KVNODE),
            "--id", node_id,
            "--port", str(port),
            "--log_path", str(log_path),
            "--hb_interval_ms", str(hb_interval),
            "--hb_timeout_ms", str(hb_timeout),
            "--repl_mode", repl_mode,
            "--run_id", run_id,
        ]
        if primary:
            cmd.append("--primary")
        if peer_addr:
            cmd.extend(["--peer", peer_addr])

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.procs.append(proc)

        self.assertTrue(
            wait_for_port(port, timeout=5.0),
            f"Node {node_id} failed to start on port {port}"
        )
        return proc

    def get_log(self, node_id) -> list:
        """Load the JSONL log for a node."""
        return load_jsonl(self.tmpdir / f"{node_id}.jsonl")

    def count_events(self, node_id, event_type) -> int:
        """Count events of a given type in a node's log."""
        return sum(1 for e in self.get_log(node_id) if e.get("event") == event_type)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: Basic KV Operations
# ═══════════════════════════════════════════════════════════════════════════════

class TestBasicKV(KVTestBase):
    """Test basic SET and GET operations on a single node."""

    PORT_OFFSET = 0

    def test_basic_set_get(self):
        """SET a key, then GET it — value should match."""
        self.start_node("node0", self.port0)

        # SET
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"hello","value":"world","req_id":"r1"}')
        obj = json.loads(resp)
        self.assertEqual(obj["type"], "KV_SET_RESP")
        self.assertEqual(obj["key"], "hello")
        self.assertTrue(obj["ok"])

        # GET
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_GET","key":"hello","req_id":"r2"}')
        obj = json.loads(resp)
        self.assertEqual(obj["type"], "KV_GET_RESP")
        self.assertEqual(obj["key"], "hello")
        self.assertEqual(obj["value"], "world")
        self.assertTrue(obj["ok"])

    def test_get_nonexistent_key(self):
        """GET a key that was never SET — should return ok=false."""
        self.start_node("node0", self.port0)

        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_GET","key":"missing","req_id":"r1"}')
        obj = json.loads(resp)
        self.assertEqual(obj["type"], "KV_GET_RESP")
        self.assertFalse(obj["ok"])
        self.assertIsNone(obj.get("value") if obj.get("value") != "null" else None)

    def test_set_overwrite(self):
        """SET a key twice — second value should overwrite."""
        self.start_node("node0", self.port0)

        # First SET
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_SET","key":"k","value":"v1","req_id":"r1"}')
        # Second SET (overwrite)
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_SET","key":"k","value":"v2","req_id":"r2"}')
        # GET should return v2
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_GET","key":"k","req_id":"r3"}')
        obj = json.loads(resp)
        self.assertEqual(obj["value"], "v2")

    def test_multiple_keys(self):
        """SET multiple distinct keys — each should GET its own value."""
        self.start_node("node0", self.port0)

        for i in range(10):
            tcp_send_recv("127.0.0.1", self.port0,
                          f'{{"type":"KV_SET","key":"key_{i}","value":"val_{i}","req_id":"s{i}"}}')

        for i in range(10):
            resp = tcp_send_recv("127.0.0.1", self.port0,
                                 f'{{"type":"KV_GET","key":"key_{i}","req_id":"g{i}"}}')
            obj = json.loads(resp)
            self.assertTrue(obj["ok"])
            self.assertEqual(obj["value"], f"val_{i}")

    def test_multiple_ops_same_connection(self):
        """Multiple SET/GET operations on a single TCP connection."""
        self.start_node("node0", self.port0)

        msgs = [
            '{"type":"KV_SET","key":"a","value":"1","req_id":"r1"}',
            '{"type":"KV_SET","key":"b","value":"2","req_id":"r2"}',
            '{"type":"KV_GET","key":"a","req_id":"r3"}',
            '{"type":"KV_GET","key":"b","req_id":"r4"}',
        ]
        responses = tcp_send_recv_multi("127.0.0.1", self.port0, msgs)

        self.assertEqual(len(responses), 4)
        # Check SET responses
        self.assertTrue(json.loads(responses[0])["ok"])
        self.assertTrue(json.loads(responses[1])["ok"])
        # Check GET responses
        self.assertEqual(json.loads(responses[2])["value"], "1")
        self.assertEqual(json.loads(responses[3])["value"], "2")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: Heartbeat and Failure Detection
# ═══════════════════════════════════════════════════════════════════════════════

class TestHeartbeat(KVTestBase):
    """Test heartbeat exchange and failure detection."""

    PORT_OFFSET = 10

    def test_heartbeat_exchange(self):
        """Primary sends heartbeats to secondary; acks are logged."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                hb_interval=50, hb_timeout=500)
        # Wait for several heartbeat exchanges
        time.sleep(1.5)

        pings = self.count_events("node0", "hb_ping_sent")
        acks = self.count_events("node0", "hb_ack_recv")

        self.assertGreater(pings, 5, "Expected at least 5 heartbeat pings")
        self.assertGreater(acks, 5, "Expected at least 5 heartbeat acks")
        # No false declarations
        self.assertEqual(self.count_events("node0", "declared_dead"), 0)

    def test_failure_detection_on_crash(self):
        """Kill secondary → primary should log declared_dead within timeout."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                hb_interval=50, hb_timeout=200)
        # Wait for warmup (grace period is 2s in the FD)
        time.sleep(2.5)

        # Verify heartbeats are working
        acks_before = self.count_events("node0", "hb_ack_recv")
        self.assertGreater(acks_before, 10, "Heartbeats should be flowing before kill")

        # Kill secondary with SIGKILL (simulate crash)
        t_kill = wall_ms()
        kill_proc(proc1, use_sigkill=True)
        self.procs.remove(proc1)

        # Wait for detection (timeout + some margin)
        time.sleep(1.5)

        # Check for declared_dead
        dead_events = [e for e in self.get_log("node0") if e.get("event") == "declared_dead"]
        self.assertEqual(len(dead_events), 1, "Expected exactly one declared_dead event")

        # Verify detection latency
        t_detect = dead_events[0]["ts_ms"]
        latency = t_detect - t_kill
        self.assertGreater(latency, 0, "Detection should happen after kill")
        self.assertLess(latency, 1000, "Detection should happen within 1 second")

    def test_no_false_positive_during_normal_operation(self):
        """During normal operation, no declared_dead should be logged."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                hb_interval=50, hb_timeout=300)
        # Run for a while
        time.sleep(3.0)

        self.assertEqual(self.count_events("node0", "declared_dead"), 0,
                         "No false positives during normal operation")
        self.assertGreater(self.count_events("node0", "hb_ack_recv"), 20)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: Synchronous Replication
# ═══════════════════════════════════════════════════════════════════════════════

class TestSyncReplication(KVTestBase):
    """Test synchronous replication: SET on primary → immediately visible on secondary."""

    PORT_OFFSET = 20

    def test_sync_repl_data_visible_on_secondary(self):
        """SET on primary with sync replication → GET on secondary returns the value."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="sync", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        # SET on primary
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"sync_key","value":"sync_val","req_id":"r1"}')
        obj = json.loads(resp)
        self.assertTrue(obj["ok"])

        # Small delay for sync replication to complete
        time.sleep(0.2)

        # GET from secondary — should have the value
        resp = tcp_send_recv("127.0.0.1", self.port1,
                             '{"type":"KV_GET","key":"sync_key","req_id":"r2"}')
        obj = json.loads(resp)
        self.assertTrue(obj["ok"], "Key should exist on secondary after sync replication")
        self.assertEqual(obj["value"], "sync_val")

    def test_sync_repl_multiple_keys(self):
        """Multiple SETs on primary → all visible on secondary."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="sync", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        # SET multiple keys on primary
        for i in range(5):
            tcp_send_recv("127.0.0.1", self.port0,
                          f'{{"type":"KV_SET","key":"rk_{i}","value":"rv_{i}","req_id":"s{i}"}}')

        time.sleep(0.3)

        # GET all from secondary
        for i in range(5):
            resp = tcp_send_recv("127.0.0.1", self.port1,
                                 f'{{"type":"KV_GET","key":"rk_{i}","req_id":"g{i}"}}')
            obj = json.loads(resp)
            self.assertTrue(obj["ok"], f"Key rk_{i} should be on secondary")
            self.assertEqual(obj["value"], f"rv_{i}")

    def test_sync_repl_logs_events(self):
        """Sync replication should log repl_start and repl_ack events on primary."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="sync", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_SET","key":"logged","value":"yes","req_id":"r1"}')
        time.sleep(0.3)

        repl_starts = self.count_events("node0", "repl_start")
        repl_acks = self.count_events("node0", "repl_ack")
        self.assertGreaterEqual(repl_starts, 1, "repl_start should be logged")
        self.assertGreaterEqual(repl_acks, 1, "repl_ack should be logged")

        # Secondary should log repl_recv
        repl_recv = self.count_events("node1", "repl_recv")
        self.assertGreaterEqual(repl_recv, 1, "repl_recv should be logged on secondary")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 4: Asynchronous Replication
# ═══════════════════════════════════════════════════════════════════════════════

class TestAsyncReplication(KVTestBase):
    """Test async replication: SET on primary → eventually visible on secondary."""

    PORT_OFFSET = 30

    def test_async_repl_eventually_visible(self):
        """SET on primary with async replication → eventually visible on secondary."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="async", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        # SET on primary
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"async_key","value":"async_val","req_id":"r1"}')
        self.assertTrue(json.loads(resp)["ok"])

        # Wait for async replication to propagate
        time.sleep(0.5)

        # GET from secondary
        resp = tcp_send_recv("127.0.0.1", self.port1,
                             '{"type":"KV_GET","key":"async_key","req_id":"r2"}')
        obj = json.loads(resp)
        self.assertTrue(obj["ok"], "Key should eventually appear on secondary")
        self.assertEqual(obj["value"], "async_val")

    def test_async_repl_primary_responds_immediately(self):
        """Async replication should not block the primary's response to the client."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="async", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        # Time the SET — it should return quickly (not wait for replication)
        t0 = time.time()
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"fast","value":"reply","req_id":"r1"}')
        t1 = time.time()
        self.assertTrue(json.loads(resp)["ok"])
        # Should complete very quickly (under 50ms is generous)
        self.assertLess(t1 - t0, 0.1, "Async SET should return without waiting for repl")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 5: No Replication
# ═══════════════════════════════════════════════════════════════════════════════

class TestNoReplication(KVTestBase):
    """Test no-replication mode: SET on primary → NOT visible on secondary."""

    PORT_OFFSET = 40

    def test_no_repl_data_not_on_secondary(self):
        """SET on primary with repl_mode=none → GET on secondary returns ok=false."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="none", hb_interval=100, hb_timeout=500)
        time.sleep(0.5)

        # SET on primary
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"local_only","value":"nope","req_id":"r1"}')
        self.assertTrue(json.loads(resp)["ok"])

        time.sleep(0.3)

        # GET from secondary — should NOT have the value
        resp = tcp_send_recv("127.0.0.1", self.port1,
                             '{"type":"KV_GET","key":"local_only","req_id":"r2"}')
        obj = json.loads(resp)
        self.assertFalse(obj["ok"], "Key should NOT be on secondary in no-replication mode")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 6: Replication + Failure Interaction
# ═══════════════════════════════════════════════════════════════════════════════

class TestReplicationFailure(KVTestBase):
    """Test replication behavior when secondary crashes."""

    PORT_OFFSET = 50

    def test_repl_skipped_after_peer_dead(self):
        """After secondary crashes, replication should be skipped (logged)."""
        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                repl_mode="sync", hb_interval=50, hb_timeout=200)
        time.sleep(2.5)  # Wait for grace period

        # SET before crash — should replicate
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"before","value":"crash","req_id":"r1"}')
        self.assertTrue(json.loads(resp)["ok"])

        # Kill secondary
        kill_proc(proc1, use_sigkill=True)
        self.procs.remove(proc1)
        time.sleep(1.5)  # Wait for detection

        # Verify declared_dead
        self.assertEqual(self.count_events("node0", "declared_dead"), 1)

        # SET after crash — primary should still accept writes
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"KV_SET","key":"after","value":"crash","req_id":"r2"}')
        obj = json.loads(resp)
        self.assertTrue(obj["ok"], "Primary should still accept writes after secondary crash")

        time.sleep(0.2)

        # Check that repl_skipped was logged
        skipped = self.count_events("node0", "repl_skipped")
        self.assertGreaterEqual(skipped, 1, "Replication should be skipped after peer death")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 7: Fault Delay Injection
# ═══════════════════════════════════════════════════════════════════════════════

class TestFaultDelay(KVTestBase):
    """Test application-level delay injection."""

    PORT_OFFSET = 60

    def test_fault_delay_injection(self):
        """Sending FAULT_DELAY message should make subsequent ops slower."""
        self.start_node("node0", self.port0)

        # Baseline latency — measure a GET
        t0 = time.time()
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_GET","key":"x","req_id":"r0"}')
        baseline_ms = (time.time() - t0) * 1000

        # Inject 200ms delay
        resp = tcp_send_recv("127.0.0.1", self.port0,
                             '{"type":"FAULT_DELAY","delay_ms":200}')
        obj = json.loads(resp)
        self.assertEqual(obj["type"], "FAULT_DELAY_ACK")
        self.assertTrue(obj["ok"])

        # Now operations should be slower
        t0 = time.time()
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_GET","key":"x","req_id":"r1"}')
        delayed_ms = (time.time() - t0) * 1000

        self.assertGreater(delayed_ms, 150,
                           f"Operation should be delayed (~200ms). Got {delayed_ms:.0f}ms")

        # Remove delay
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"FAULT_DELAY","delay_ms":0}')


# ═══════════════════════════════════════════════════════════════════════════════
# Test 8: Log Format Correctness
# ═══════════════════════════════════════════════════════════════════════════════

class TestLogFormat(KVTestBase):
    """Test that logs are well-formed JSONL with required fields."""

    PORT_OFFSET = 70

    def test_log_has_required_fields(self):
        """Every log line should have ts_ms, node_id, run_id, event."""
        self.start_node("node0", self.port0, run_id="test_log_format")
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_SET","key":"k","value":"v","req_id":"r1"}')
        time.sleep(0.2)

        events = self.get_log("node0")
        self.assertGreater(len(events), 0, "Should have at least one log event")

        required_fields = ["ts_ms", "node_id", "run_id", "event"]
        for evt in events:
            for field in required_fields:
                self.assertIn(field, evt, f"Log event missing field '{field}': {evt}")

    def test_log_node_start_event(self):
        """First event should be node_start with configuration info."""
        self.start_node("node0", self.port0, run_id="test_node_start")
        time.sleep(0.2)

        events = self.get_log("node0")
        self.assertGreater(len(events), 0)

        first = events[0]
        self.assertEqual(first["event"], "node_start")
        self.assertEqual(first["node_id"], "node0")
        self.assertEqual(first["run_id"], "test_node_start")
        self.assertIn("extra", first)
        self.assertIn("port", first["extra"])

    def test_log_timestamps_are_monotonic(self):
        """Log timestamps should be non-decreasing."""
        self.start_node("node0", self.port0)

        # Generate some activity
        for i in range(5):
            tcp_send_recv("127.0.0.1", self.port0,
                          f'{{"type":"KV_SET","key":"k{i}","value":"v{i}","req_id":"r{i}"}}')
        time.sleep(0.2)

        events = self.get_log("node0")
        timestamps = [e["ts_ms"] for e in events]
        for i in range(1, len(timestamps)):
            self.assertGreaterEqual(timestamps[i], timestamps[i - 1],
                                    "Timestamps should be non-decreasing")

    def test_log_client_events(self):
        """SET operation should generate client_req_recv and client_req_done."""
        self.start_node("node0", self.port0)
        tcp_send_recv("127.0.0.1", self.port0,
                      '{"type":"KV_SET","key":"k","value":"v","req_id":"r1"}')
        time.sleep(0.2)

        self.assertGreaterEqual(self.count_events("node0", "client_req_recv"), 1)
        self.assertGreaterEqual(self.count_events("node0", "client_req_done"), 1)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 9: Workload Generator
# ═══════════════════════════════════════════════════════════════════════════════

class TestWorkloadGenerator(KVTestBase):
    """Test kv_workload binary produces valid output."""

    PORT_OFFSET = 80

    def test_workload_basic(self):
        """Workload generator should complete and produce valid logs."""
        if not KV_WORKLOAD.exists():
            self.skipTest("kv_workload binary not found")

        self.start_node("node0", self.port0)
        time.sleep(0.3)

        wl_log = self.tmpdir / "workload.jsonl"
        proc = subprocess.run(
            [str(KV_WORKLOAD),
             "--target", f"127.0.0.1:{self.port0}",
             "--num_ops", "20",
             "--set_ratio", "0.5",
             "--rate", "200",
             "--log_path", str(wl_log),
             "--run_id", "wl_test"],
            timeout=10,
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 0, f"Workload failed: {proc.stderr.decode()}")

        events = load_jsonl(wl_log)
        self.assertGreater(len(events), 0, "Workload should produce log events")

        # Count op_start and op_done
        op_starts = [e for e in events if e.get("event") == "op_start"]
        op_dones = [e for e in events if e.get("event") == "op_done"]
        self.assertEqual(len(op_starts), 20, "Should have 20 op_start events")
        self.assertEqual(len(op_dones), 20, "Should have 20 op_done events")

        # All ops should be successful
        for e in op_dones:
            self.assertTrue(e.get("success"), f"Operation failed: {e}")

        # op_done should have latency_us
        for e in op_dones:
            self.assertIn("latency_us", e)
            self.assertGreater(e["latency_us"], 0)

    def test_workload_set_ratio(self):
        """Workload with set_ratio=1.0 should produce only SETs."""
        if not KV_WORKLOAD.exists():
            self.skipTest("kv_workload binary not found")

        self.start_node("node0", self.port0)
        time.sleep(0.3)

        wl_log = self.tmpdir / "workload_sets.jsonl"
        subprocess.run(
            [str(KV_WORKLOAD),
             "--target", f"127.0.0.1:{self.port0}",
             "--num_ops", "10",
             "--set_ratio", "1.0",
             "--log_path", str(wl_log),
             "--run_id", "wl_sets"],
            timeout=10,
            capture_output=True,
        )

        events = load_jsonl(wl_log)
        op_events = [e for e in events if e.get("event") == "op_start"]
        set_ops = [e for e in op_events if e.get("op_type") == "SET"]
        self.assertEqual(len(set_ops), 10, "All ops should be SETs when set_ratio=1.0")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 10: Concurrent Clients
# ═══════════════════════════════════════════════════════════════════════════════

class TestConcurrentClients(KVTestBase):
    """Test that two clients can operate concurrently."""

    PORT_OFFSET = 90

    def test_two_concurrent_connections(self):
        """Two TCP connections sending SET/GET concurrently."""
        self.start_node("node0", self.port0)
        time.sleep(0.3)

        import threading
        results = {"c1": [], "c2": []}

        def client_work(client_id, keys):
            try:
                for k in keys:
                    resp = tcp_send_recv(
                        "127.0.0.1", self.port0,
                        f'{{"type":"KV_SET","key":"{k}","value":"{client_id}","req_id":"{client_id}_{k}"}}')
                    results[client_id].append(json.loads(resp))
            except Exception as e:
                results[client_id].append({"error": str(e)})

        t1 = threading.Thread(target=client_work, args=("c1", [f"c1_key_{i}" for i in range(5)]))
        t2 = threading.Thread(target=client_work, args=("c2", [f"c2_key_{i}" for i in range(5)]))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        # Both clients should have 5 successful responses
        for cid in ["c1", "c2"]:
            self.assertEqual(len(results[cid]), 5, f"Client {cid} should have 5 responses")
            for r in results[cid]:
                self.assertNotIn("error", r, f"Client {cid} had an error: {r}")
                self.assertTrue(r.get("ok"), f"Client {cid} SET failed: {r}")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 11: Detection Latency Bounds
# ═══════════════════════════════════════════════════════════════════════════════

class TestDetectionLatencyBounds(KVTestBase):
    """Test that detection latency is bounded by theoretical expectations."""

    PORT_OFFSET = 100

    def test_detection_within_timeout_plus_interval(self):
        """Detection latency should be ≤ hb_timeout + hb_interval + margin."""
        hb_interval = 50
        hb_timeout = 200

        proc1 = self.start_node("node1", self.port1)
        time.sleep(0.3)
        proc0 = self.start_node("node0", self.port0, primary=True,
                                peer_addr=f"127.0.0.1:{self.port1}",
                                hb_interval=hb_interval, hb_timeout=hb_timeout)
        time.sleep(2.5)  # Grace period

        # Kill secondary
        t_kill = wall_ms()
        kill_proc(proc1, use_sigkill=True)
        self.procs.remove(proc1)
        time.sleep(2.0)

        dead_events = [e for e in self.get_log("node0") if e.get("event") == "declared_dead"]
        self.assertEqual(len(dead_events), 1, "Expected declared_dead")

        latency_ms = dead_events[0]["ts_ms"] - t_kill
        # Theoretical upper bound: timeout + interval + scheduling jitter
        upper_bound = hb_timeout + hb_interval + 100  # 100ms margin for scheduling
        self.assertLessEqual(latency_ms, upper_bound,
                             f"Detection latency {latency_ms}ms exceeds bound {upper_bound}ms")
        self.assertGreater(latency_ms, 0, "Detection latency should be positive")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Check binary exists before running
    if not KVNODE.exists():
        print(f"ERROR: Binary not found: {KVNODE}")
        print(f"Build first: cd {ROOT} && mkdir -p build && cd build && cmake .. && make")
        sys.exit(1)

    unittest.main(verbosity=2)
