# Minimal 2-Node KV Store with Heartbeat Failure Detection — Implementation Specification

Implementation-ready design doc for a 2-node in-memory KV store used to quantify failure detection latency. No replication, no consensus, no leader election.

---

## 1) Overview

**Purpose:** Build a minimal distributed KV store where Node A (detector) monitors Node B (monitored) via heartbeats and a configurable timeout, so we can run experiments on a single machine and measure *detection latency* (time from B’s death until A declares B dead). The system exposes GET/SET and a heartbeat protocol; the main deliverable is reproducible data for two plots: (1) a 2D heatmap of median detection latency over `(hb_interval_ms, hb_timeout_ms)`, and (2) a scatter plot of median detection latency vs. missed heartbeats `hb_timeout_ms / hb_interval_ms`.

**Non-goals:** Replication, consensus, leader election, multi-node membership, or production-grade durability. Recovery after B comes back is optional and can be out-of-scope for the first version.

---

## 2) Components & Responsibilities

### Node A (detector)
- Listens for KV requests (GET/SET) on its HTTP (or TCP) port; serves from its own in-memory map only (no replication to B).
- Runs the failure-detection loop: sends HEARTBEAT_PING to B every `hb_interval_ms`, tracks `last_ack_time`, declares B dead when `now - last_ack_time >= hb_timeout_ms`.
- Logs all FD events (ping sent, ack received, declared_dead) to JSONL with timestamps (see §5: use wall clock for experiment alignment).
- Does not route KV traffic to B; B is only used as a heartbeat target.

### Node B (monitored)
- Listens for KV requests on its port (optional; can be stub if we only need FD).
- Listens for heartbeats from A; on each HEARTBEAT_PING, replies with HEARTBEAT_ACK immediately.
- No failure-detection logic; it only responds to pings.

### KV request handling (client → node)
- Client sends GET/SET to a single node (A or B). That node serves from its local in-memory map. No cross-node forwarding for KV in this minimal design.

### Failure detector loop (on A only)
- Single dedicated thread or async loop: every `hb_interval_ms`, send HEARTBEAT_PING to B; on receipt of HEARTBEAT_ACK, update `last_ack_time`; every check period (e.g. every 10 ms or every ping), if `now - last_ack_time >= hb_timeout_ms` then emit `declared_dead` once and optionally stop sending pings (or keep sending for future recovery).

### Timestamps
- **A’s internal timeout:** Use `CLOCK_MONOTONIC` for the condition `now - last_ack_time >= hb_timeout_ms` so NTP/sleep do not affect the decision.
- **A’s log `ts_ms` and injector:** Use wall clock (ms since epoch) for all experiment logs so t_fail and t_detect are directly comparable (see §5).

---

## 3) Network Protocols

### Message types (minimal fields)

**HEARTBEAT_PING**
- `type`: "HEARTBEAT_PING"
- `seq`: uint64 (optional; useful for debugging duplicate ACKs)
- `ts_ms`: int64 (optional; sender’s monotonic ms when sent)

**HEARTBEAT_ACK**
- `type`: "HEARTBEAT_ACK"
- `seq`: uint64 (same as ping; optional)
- `ts_ms`: int64 (optional; B’s time when ack sent)

**KV_GET request**
- `type`: "KV_GET"
- `key`: string

**KV_GET response**
- `type`: "KV_GET_RESP"
- `key`: string
- `value`: string | null
- `ok`: bool

**KV_SET request**
- `type`: "KV_SET"
- `key`: string
- `value`: string

**KV_SET response**
- `type`: "KV_SET_RESP"
- `key`: string
- `ok`: bool

Transport: **TCP**. Reason: ordered, reliable delivery so we don’t confuse lost ACKs with network loss; simpler for a single-machine experiment. UDP would require custom retries and duplicate handling.

Timeouts/retry: No application-level retry for heartbeats; the timeout itself is the “retry” (declare dead after `hb_timeout_ms`). Optional: TCP connect/socket read timeout of e.g. `hb_timeout_ms` so a dead B doesn’t block the detector forever.

---

## 4) Failure Detection Algorithm (precise)

**Pseudocode (Node A):**

```text
// At startup
last_ack_time := now_monotonic_ms()
dead_declared := false

// Loop (runs every hb_interval_ms, or tick at hb_interval_ms and check every 10ms)
every hb_interval_ms:
    send HEARTBEAT_PING to B
    log hb_ping_sent with ts_ms = now_wall_ms()   // wall clock for experiment alignment

on HEARTBEAT_ACK from B:
    last_ack_time := now_monotonic_ms()          // monotonic for timeout math
    log hb_ack_recv with ts_ms = now_wall_ms()

every check_interval_ms (e.g. 10ms):
    if not dead_declared and (now_monotonic_ms() - last_ack_time >= hb_timeout_ms):
        dead_declared := true
        log declared_dead with ts_ms = now_wall_ms()
        // optional: stop sending pings; or keep sending for recovery
```

**Double-declaration:** Use a single boolean `dead_declared`. Once set, do not log `declared_dead` again until process restart (or until an explicit “recovery” phase if you add it later).

**Recovery:** Out-of-scope for the minimal spec. If later you want recovery: when A receives an ACK after having declared B dead, set `dead_declared := false` and update `last_ack_time`; optionally log `declared_alive`.

---

## 5) Experiment Timing Definitions (precise)

- **t_fail:** Wall time (or monotonic time, see below) when B is killed by the injector. The injector records this in `injector.jsonl`.
- **t_detect:** Time when A logs the first `declared_dead` event. Taken from A’s JSONL `ts_ms` for that event.
- **detection_latency_ms** = t_detect - t_fail (both in the same time base).

**Clock choice:** Use **CLOCK_MONOTONIC** for A’s internal time and for A’s log `ts_ms`. Then:
- **Alignment on one machine:** Have the injector record B’s death time using the same monotonic clock. On Linux/macOS, the injector can read `/proc/self/stat` or use a small C/Python helper that calls `clock_gettime(CLOCK_MONOTONIC)` and write that into `injector.jsonl`. A’s process started earlier, so A’s monotonic zero is different from the injector’s. So we **cannot** mix A’s monotonic with injector’s monotonic directly.
- **Practical approach:** Use **wall clock** for *experiment* timestamps only: injector and A both record wall time (ms since epoch). t_fail = injector’s wall time when it kills B; t_detect = A’s log wall time for `declared_dead`. Then detection_latency_ms = t_detect - t_fail. Ensure NTP is enabled and no big clock steps during the short run. Alternatively: injector starts A, waits for A to be ready, then injector asks A (via a small admin socket or HTTP) “what is your current monotonic ms?” and records the mapping (wall_time, monotonic_ms); when injector kills B it records wall time and later converts t_detect from A’s log (if A logged monotonic) to wall using that mapping. Simplest is: **everyone logs wall time** for experiment events so t_fail and t_detect are directly comparable; document that clock skew on a single machine is usually &lt; 1 ms.

**Summary:** Use wall clock (ms since epoch) for `ts_ms` in experiment logs and injector so t_fail and t_detect align. Use monotonic internally inside A for timeout math if you prefer (then convert to wall for the one log line that matters: `declared_dead`).

---

## 6) Logging Specification (JSONL required)

**Schema (one JSON object per line):**

| Field         | Type   | Required | Description |
|---------------|--------|----------|-------------|
| ts_ms         | int64  | yes      | Timestamp (wall ms since epoch, or monotonic ms; must be consistent) |
| node_id       | string | yes      | "A" or "B" |
| run_id        | string | yes      | Unique run identifier (e.g. from injector) |
| hb_interval_ms| int    | yes      | Heartbeat interval in ms |
| hb_timeout_ms | int    | yes      | Timeout in ms |
| event         | string | yes      | Event type (see enum below) |
| peer_id       | string | no       | For FD: the peer (e.g. "B" when A logs) |
| extra         | object | no       | Event-specific data |

**Event enum (minimum):**
- `hb_ping_sent`
- `hb_ack_recv`
- `declared_dead`
- `kv_get`, `kv_set`, `kv_resp` (optional)

**Example lines:**

```json
{"ts_ms":1699900000123,"node_id":"A","run_id":"run_001","hb_interval_ms":100,"hb_timeout_ms":400,"event":"hb_ping_sent","peer_id":"B","extra":{}}
{"ts_ms":1699900000150,"node_id":"A","run_id":"run_001","hb_interval_ms":100,"hb_timeout_ms":400,"event":"hb_ack_recv","peer_id":"B","extra":{}}
{"ts_ms":1699900001520,"node_id":"A","run_id":"run_001","hb_interval_ms":100,"hb_timeout_ms":400,"event":"declared_dead","peer_id":"B","extra":{}}
{"ts_ms":1699900001000,"node_id":"A","run_id":"run_001","hb_interval_ms":100,"hb_timeout_ms":400,"event":"kv_get","peer_id":null,"extra":{"key":"x"}}
{"ts_ms":1699900001001,"node_id":"A","run_id":"run_001","hb_interval_ms":100,"hb_timeout_ms":400,"event":"kv_resp","peer_id":null,"extra":{"key":"x","ok":true,"value":"42"}}
```

---

## 7) CLI Interface (exact flags)

**Common (both nodes):**
- `--id`          string   Node id (e.g. "A", "B")
- `--port`        int      Port to listen on (KV + heartbeat or heartbeat only)
- `--log_path`    string   Path to JSONL log file
- `--hb_interval_ms` int   Heartbeat interval in ms
- `--hb_timeout_ms`  int   Timeout in ms (used only by detector)
- `--role`        string   "detector" | "monitored"

**Detector only:**
- `--peer_addr`   string   Address of monitored node, e.g. "127.0.0.1:8002"

**Optional:**
- `--run_id`      string   If not set, can be generated or left empty until injector sets it via env.

**Example — Node A (detector), Node B (monitored):**

```bash
# Terminal 1 — B first (monitored)
./kvnode --id B --port 8002 --role monitored --log_path /tmp/b.jsonl --hb_interval_ms 100 --hb_timeout_ms 400

# Terminal 2 — A (detector)
./kvnode --id A --port 8001 --role detector --peer_addr 127.0.0.1:8002 --log_path /tmp/a.jsonl --hb_interval_ms 100 --hb_timeout_ms 400
```

If a single binary serves both roles, `--role` selects behavior; B ignores `--peer_addr`, A requires `--peer_addr`.

---

## 8) Failure Injection Tooling

**Minimal injector behavior (outline):**

1. Generate `run_id` (e.g. `fd_run_${hb_interval}_${hb_timeout}_${timestamp}`).
2. Start B first, then A; pass `run_id` via env or config so both log the same `run_id`.
3. **Warmup:** Sleep W seconds (e.g. 2–3) so that A has sent several pings and received ACKs.
4. **Record t_fail:** Just before killing B, record `t_fail = current_time_ms()` (wall clock). Write to `injector.jsonl`: `{"event":"kill_b","ts_ms":t_fail,"run_id":"..."}`. Then kill B (e.g. SIGKILL).
5. **Wait for declaration:** Poll A’s log file for a line with `event":"declared_dead"` and same `run_id`; parse `ts_ms` as t_detect. Optionally timeout after e.g. 2 * hb_timeout_ms.
6. **Save injector.jsonl:** Append (or write) at least: `t_fail`, `run_id`, and optionally `t_detect`, `detection_latency_ms` so the aggregator can use either raw logs or this summary.

**Time base:** Use wall clock (ms since epoch) for both injector and A’s `ts_ms` so no conversion is needed. Ensure injector and A run on the same machine and that the injector reads A’s log from the same filesystem so there’s no systematic offset.

**Example injector.jsonl:**

```json
{"event":"run_start","run_id":"fd_run_100_400_1699900000","ts_ms":1699900000000}
{"event":"kill_b","run_id":"fd_run_100_400_1699900000","ts_ms":1699900002500}
{"event":"declared_dead","run_id":"fd_run_100_400_1699900000","ts_ms":1699900002890,"detection_latency_ms":390}
```

---

## 9) Data Aggregation Outputs (for plotting)

**Trials:** Recommend at least **5–10 trials** per `(hb_interval_ms, hb_timeout_ms)` cell so median and IQR are stable.

**Heatmap CSV:**  
- Rows: unique `hb_timeout_ms` (sorted).  
- Columns: unique `hb_interval_ms` (sorted).  
- Value: `median_detection_ms` (and optionally a second matrix or extra columns for `iqr_detection_ms`).  
- Example filename: `heatmap_median.csv` or `heatmap.csv` with columns `hb_timeout_ms,hb_interval_ms,median_detection_ms,iqr_detection_ms`.

**Scatter CSV:**  
- Columns: `missed`, `hb_interval_ms`, `hb_timeout_ms`, `median_detection_ms`, `iqr_detection_ms` (optional).  
- `missed` = `hb_timeout_ms / hb_interval_ms` (can be float). One row per parameter combination (aggregated over trials).  
- Example filename: `scatter.csv`.

**Summary statistics:** For each `(hb_interval_ms, hb_timeout_ms)` compute over trials: median and IQR of `detection_latency_ms`. Optionally: min, max, count.

---

## 10) Edge Cases & Validation

**Things that could go wrong and mitigations:**

1. **Scheduling jitter:** A’s ping loop or check loop is delayed by OS scheduling, so detection is late. Mitigation: run with nice priority, reduce background load; report IQR to see variance.
2. **Late ACK after dead declaration:** B was slow to die and an ACK arrives after A declared B dead. Mitigation: ignore ACKs (or only update `last_ack_time`) and do not “undeclare” in the minimal spec; optional: ignore ACKs once `dead_declared` is true.
3. **Log flush:** A has declared B dead but the line is not yet on disk when injector reads the log. Mitigation: flush after each critical log line (e.g. `declared_dead`); or poll the log file with short sleeps until the line appears (with timeout).
4. **Clock mismatch:** If injector and A used different clocks, t_detect - t_fail is wrong. Mitigation: use the same wall clock for both and same machine; document that assumption.
5. **Duplicate declared_dead:** Bug in logic emits multiple declared_dead. Mitigation: single `dead_declared` flag and only one log; aggregator takes first `declared_dead` per run_id only.

**Sanity check checklist (before parameter sweeps):**

- [ ] A and B start; A’s log shows repeated `hb_ping_sent` and `hb_ack_recv` with ~hb_interval_ms spacing.
- [ ] Kill B; within roughly hb_timeout_ms + one hb_interval_ms, A logs exactly one `declared_dead`.
- [ ] detection_latency_ms is positive and on the order of hb_timeout_ms (not negative, not hours).
- [ ] One run: manually compare injector’s t_fail and A’s declared_dead ts_ms; difference equals detection_latency_ms within a few ms.
- [ ] KV GET/SET on A or B return expected values (optional if KV is secondary).

---

*End of specification.*
