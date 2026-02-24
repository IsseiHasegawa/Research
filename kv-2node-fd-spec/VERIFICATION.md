# Post-Implementation Verification

Run this checklist after implementing the 2-node KV + failure detector to ensure correctness before running experiments.

## 1. Spec coverage

- [ ] **§1 Overview** — Implemented exactly 2 nodes (A detector, B monitored); no replication/consensus/leader election.
- [ ] **§2 Components** — A runs FD loop and logs; B only responds to pings; KV is in-memory per node.
- [ ] **§3 Protocols** — HEARTBEAT_PING / HEARTBEAT_ACK (and optional KV_GET/SET) over TCP; message fields match spec.
- [ ] **§4 Algorithm** — Ping every `hb_interval_ms`; `last_ack_time` updated on ACK; declare dead when `now - last_ack_time >= hb_timeout_ms`; no double-declare (single `dead_declared` flag).
- [ ] **§5 Timing** — t_fail (injector), t_detect (A’s log); detection_latency_ms = t_detect - t_fail; same time base (wall clock) for both.
- [ ] **§6 Logging** — JSONL with required fields; events `hb_ping_sent`, `hb_ack_recv`, `declared_dead` (and optional kv_*).
- [ ] **§7 CLI** — Flags `--id`, `--port`, `--peer_addr`, `--hb_interval_ms`, `--hb_timeout_ms`, `--log_path`, `--role`; example commands run A and B locally.
- [ ] **§8 Injector** — Starts A and B, warmup, records t_fail, kills B, waits for declared_dead, writes injector.jsonl with same time base.
- [ ] **§9 Aggregation** — Heatmap CSV (rows=hb_timeout_ms, cols=hb_interval_ms, value=median_detection_ms; optional IQR); Scatter CSV (missed, hb_interval_ms, hb_timeout_ms, median_detection_ms); trials per setting (e.g. 5–10); median + IQR.
- [ ] **§10 Edge cases** — Addressed: jitter, late ACK, log flush, clock alignment, duplicate declared_dead.

## 2. Sanity run

- [ ] Start B then A with same `run_id` (or injector-set).
- [ ] A’s log shows repeated `hb_ping_sent` and `hb_ack_recv` at ~`hb_interval_ms`.
- [ ] Kill B; A logs exactly one `declared_dead` within ~hb_timeout_ms + hb_interval_ms.
- [ ] detection_latency_ms from injector (or aggregator) is positive and on the order of hb_timeout_ms.
- [ ] Optional: GET/SET on A or B work.

## 3. Plot inputs

- [ ] Heatmap CSV and scatter CSV are produced by the aggregator and match the column definitions in §9.

If all items are checked, the implementation is ready for parameter sweeps and plotting.
