/**
 * workload.cpp — Workload generator for the distributed KV store.
 *
 * Connects to a target node and sends SET/GET operations at a configurable
 * rate. Records per-operation timing to a JSONL log for latency and
 * throughput analysis.
 *
 * Usage:
 *   ./kv_workload --target 127.0.0.1:9100 \
 *                 --num_ops 500 \
 *                 --set_ratio 0.5 \
 *                 --rate 100 \
 *                 --log_path output/logs/workload.jsonl \
 *                 --run_id experiment_001
 *
 * Output log format (one JSON object per line):
 *   {"ts_ms":..., "event":"op_start"|"op_done", "op_type":"SET"|"GET",
 *    "key":"...", "req_id":"...", "latency_us":..., "success":true|false}
 */

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../common/message.hpp"
#include "../common/net.hpp"

// ─── Timing ───────────────────────────────────────────────────────────────────

static int64_t wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static int64_t steady_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// ─── Simple PRNG (xorshift64) ─────────────────────────────────────────────────
// Lightweight, deterministic, no external dependency.

static uint64_t rng_state = 12345;

static uint64_t xorshift64() {
    uint64_t x = rng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    rng_state = x;
    return x;
}

// ─── Log writer ───────────────────────────────────────────────────────────────

struct WorkloadLogger {
    std::ofstream f;
    std::string run_id;

    bool open(const std::string& path) {
        f.open(path, std::ios::out | std::ios::app);
        return f.is_open();
    }

    void log_op(const std::string& event, const std::string& op_type,
                const std::string& key, const std::string& req_id,
                int64_t latency_us = -1, bool success = true) {
        if (!f.is_open()) return;
        f << "{\"ts_ms\":" << wall_ms()
          << ",\"run_id\":\"" << msg::json_escape(run_id)
          << "\",\"event\":\"" << msg::json_escape(event)
          << "\",\"op_type\":\"" << msg::json_escape(op_type)
          << "\",\"key\":\"" << msg::json_escape(key)
          << "\",\"req_id\":\"" << msg::json_escape(req_id) << "\"";
        if (latency_us >= 0) {
            f << ",\"latency_us\":" << latency_us;
        }
        f << ",\"success\":" << (success ? "true" : "false") << "}\n";
        f.flush();
    }
};

// ─── Usage ────────────────────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "\n"
        << "Required:\n"
        << "  --target <host:port>   Target node address\n"
        << "  --log_path <path>      JSONL log file path\n"
        << "\n"
        << "Optional:\n"
        << "  --num_ops <n>          Number of operations (default: 100)\n"
        << "  --set_ratio <0.0-1.0>  Fraction of SET operations (default: 0.5)\n"
        << "  --rate <ops/sec>       Target ops/sec, 0=unlimited (default: 0)\n"
        << "  --run_id <id>          Experiment run identifier (default: from $RUN_ID)\n"
        << "  --key_space <n>        Number of distinct keys to use (default: 50)\n";
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    std::string target;
    std::string log_path;
    std::string run_id = "default_run";
    int num_ops = 100;
    double set_ratio = 0.5;
    int rate = 0;  // ops/sec, 0 = unlimited
    int key_space = 50;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--target" && i + 1 < argc) target = argv[++i];
        else if (arg == "--log_path" && i + 1 < argc) log_path = argv[++i];
        else if (arg == "--run_id" && i + 1 < argc) run_id = argv[++i];
        else if (arg == "--num_ops" && i + 1 < argc) num_ops = std::stoi(argv[++i]);
        else if (arg == "--set_ratio" && i + 1 < argc) set_ratio = std::stod(argv[++i]);
        else if (arg == "--rate" && i + 1 < argc) rate = std::stoi(argv[++i]);
        else if (arg == "--key_space" && i + 1 < argc) key_space = std::stoi(argv[++i]);
        else if (arg == "--help" || arg == "-h") { usage(argv[0]); return 0; }
    }

    // Read run_id from env if not set
    if (run_id == "default_run") {
        const char* env = std::getenv("RUN_ID");
        if (env && env[0] != '\0') run_id = env;
    }

    if (target.empty() || log_path.empty()) {
        std::cerr << "Error: --target and --log_path are required\n\n";
        usage(argv[0]);
        return 1;
    }

    // Parse target address
    std::string host;
    int port;
    if (!net::parse_addr(target, host, port)) {
        std::cerr << "Error: invalid target address: " << target << "\n";
        return 1;
    }

    // Connect to target node
    int fd = net::tcp_connect(host, port, 10, 500);
    if (fd == net::INVALID_FD) {
        std::cerr << "Error: failed to connect to " << target << "\n";
        return 1;
    }
    std::cerr << "workload: connected to " << target << "\n";

    // Open log
    WorkloadLogger logger;
    logger.run_id = run_id;
    if (!logger.open(log_path)) {
        std::cerr << "Error: failed to open log " << log_path << "\n";
        net::close_fd(fd);
        return 1;
    }

    // Calculate inter-operation delay
    int64_t delay_us = 0;
    if (rate > 0) {
        delay_us = 1000000 / rate;
    }

    // Seed RNG with something semi-unique
    rng_state = static_cast<uint64_t>(wall_ms()) ^ 0xDEADBEEF;

    // Run workload
    std::vector<char> recv_buf;
    int success_count = 0;
    int fail_count = 0;

    for (int i = 0; i < num_ops; ++i) {
        // Decide SET or GET
        bool is_set = (static_cast<double>(xorshift64() % 10000) / 10000.0) < set_ratio;
        std::string op_type = is_set ? "SET" : "GET";

        // Pick a key
        int key_idx = static_cast<int>(xorshift64() % static_cast<uint64_t>(key_space));
        std::string key = "key_" + std::to_string(key_idx);
        std::string req_id = "r_" + std::to_string(i);
        std::string value = "val_" + std::to_string(i);

        // Build request
        std::string request;
        if (is_set) {
            request = msg::make_kv_set(key, value, req_id);
        } else {
            request = msg::make_kv_get(key, req_id);
        }

        // Send and time the operation
        logger.log_op("op_start", op_type, key, req_id);
        int64_t t0 = steady_us();

        bool ok = net::send_msg(fd, request);
        if (ok) {
            // Read response
            std::string resp_line;
            ok = net::recv_line(fd, &resp_line, recv_buf);
        }

        int64_t t1 = steady_us();
        int64_t latency = t1 - t0;

        if (ok) {
            success_count++;
            logger.log_op("op_done", op_type, key, req_id, latency, true);
        } else {
            fail_count++;
            logger.log_op("op_done", op_type, key, req_id, latency, false);

            // Try to reconnect
            net::close_fd(fd);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            fd = net::tcp_connect(host, port, 3, 200);
            if (fd == net::INVALID_FD) {
                std::cerr << "workload: reconnect failed, aborting remaining ops\n";
                break;
            }
            recv_buf.clear();
            std::cerr << "workload: reconnected after failure at op " << i << "\n";
        }

        // Rate limiting
        if (delay_us > 0) {
            int64_t elapsed = steady_us() - t0;
            if (elapsed < delay_us) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(delay_us - elapsed));
            }
        }
    }

    net::close_fd(fd);

    std::cerr << "workload: done. success=" << success_count
              << " fail=" << fail_count << "\n";

    return (fail_count > 0 && success_count == 0) ? 1 : 0;
}
