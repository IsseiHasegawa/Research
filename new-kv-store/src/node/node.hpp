/**
 * node.hpp — Main node logic for the distributed KV store.
 *
 * A Node manages:
 *   - A TCP server accepting client connections and peer connections
 *   - An in-memory KVStore
 *   - A FailureDetector monitoring one peer (in the 2-node setup)
 *   - A Replicator for forwarding writes to the secondary
 *   - Artificial delay injection for fault experiments
 *
 * Architecture (2-node, single machine):
 *
 *   Node 0 (primary):
 *     - Accepts client SET/GET requests
 *     - On SET: writes locally, replicates to Node 1, responds to client
 *     - Connects to Node 1 for heartbeat monitoring (FailureDetector)
 *     - Connects to Node 1 for replication (Replicator)
 *
 *   Node 1 (secondary):
 *     - Accepts incoming peer connections (heartbeat + replication)
 *     - Receives REPL_SET → writes to local store → replies REPL_SET_ACK
 *     - Responds to HEARTBEAT_PING with HEARTBEAT_ACK
 *     - Can also accept client GET/SET (for data-loss verification)
 *
 * Extension to 3+ nodes:
 *   - Add more entries to the --peers list
 *   - Each node would run one FailureDetector per peer
 *   - Replication would need to target multiple secondaries
 *   - This is documented as future work
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../common/logger.hpp"
#include "../common/message.hpp"
#include "../common/net.hpp"
#include "../failure_detector/heartbeat.hpp"
#include "../replication/replicator.hpp"
#include "kv_store.hpp"

namespace kvs {

/**
 * Configuration for a Node, parsed from CLI arguments.
 */
struct NodeConfig {
    std::string id;               // e.g. "node0", "node1"
    int port = 9100;              // Listen port for client + peer connections
    std::string peer_addr;        // "host:port" of the peer (empty if no peer)
    std::string log_path;         // JSONL log file path
    std::string run_id = "default_run";
    int hb_interval_ms = 100;
    int hb_timeout_ms = 400;
    std::string repl_mode = "none";  // "none", "sync", "async"
    bool is_primary = false;         // Primary initiates heartbeat + replication
};

class Node {
public:
    explicit Node(const NodeConfig& cfg)
        : cfg_(cfg)
        , running_(false)
        , injected_delay_ms_(0)
    {}

    /**
     * Run the node. This blocks until stop() is called or the process is
     * terminated via signal.
     */
    void run() {
        // Set up logger
        if (!logger_.open(cfg_.log_path)) {
            std::cerr << cfg_.id << ": failed to open log " << cfg_.log_path << "\n";
            return;
        }
        logger_.set_metadata(cfg_.id, cfg_.run_id);
        logger_.log("node_start", "",
                    "{\"port\":" + std::to_string(cfg_.port) +
                    ",\"repl_mode\":\"" + cfg_.repl_mode +
                    "\",\"is_primary\":" + (cfg_.is_primary ? "true" : "false") +
                    ",\"hb_interval_ms\":" + std::to_string(cfg_.hb_interval_ms) +
                    ",\"hb_timeout_ms\":" + std::to_string(cfg_.hb_timeout_ms) + "}");

        // Start TCP listener
        listen_fd_ = net::tcp_listen(cfg_.port);
        if (listen_fd_ == net::INVALID_FD) {
            std::cerr << cfg_.id << ": failed to bind port " << cfg_.port << "\n";
            return;
        }
        std::cerr << cfg_.id << ": listening on port " << cfg_.port << "\n";
        running_.store(true);

        // If primary: connect to peer for heartbeat and replication
        if (cfg_.is_primary && !cfg_.peer_addr.empty()) {
            start_primary_connections();
        }

        // Accept loop: handle incoming connections (clients and peers)
        run_accept_loop();

        logger_.log("node_stop");
    }

    void stop() {
        running_.store(false);
        // Close the listen socket to unblock accept()
        net::close_fd(listen_fd_);
        listen_fd_ = net::INVALID_FD;

        if (fd_) {
            fd_->stop();
        }
        if (replicator_) {
            replicator_->stop();
        }
    }

    // Called externally (e.g. from signal handler) to flag shutdown.
    static std::atomic<bool>& global_shutdown() {
        static std::atomic<bool> flag{false};
        return flag;
    }

private:
    // ─── Primary initialization ───────────────────────────────────────────────

    void start_primary_connections() {
        std::string host;
        int port;
        if (!net::parse_addr(cfg_.peer_addr, host, port)) {
            std::cerr << cfg_.id << ": invalid peer_addr: " << cfg_.peer_addr << "\n";
            return;
        }

        // Connect for heartbeat (failure detection)
        int hb_fd = net::tcp_connect(host, port);
        if (hb_fd == net::INVALID_FD) {
            std::cerr << cfg_.id << ": failed to connect to peer " << cfg_.peer_addr
                      << " for heartbeat\n";
            return;
        }
        std::cerr << cfg_.id << ": connected to peer " << cfg_.peer_addr
                  << " for heartbeat\n";

        std::string peer_id = "peer";  // We use a generic id; in 2-node setup this is the secondary

        fd_ = std::make_unique<FailureDetector>(
            cfg_.id, peer_id, cfg_.hb_interval_ms, cfg_.hb_timeout_ms, logger_);

        // When peer is declared dead, notify replicator
        fd_->on_peer_dead([this]() {
            if (replicator_) {
                replicator_->set_peer_dead();
            }
        });
        fd_->start(hb_fd);

        // Connect for replication (separate connection)
        if (cfg_.repl_mode != "none") {
            replicator_ = std::make_unique<Replicator>(
                cfg_.repl_mode, cfg_.id, peer_id, logger_);
            if (!replicator_->connect(host, port)) {
                std::cerr << cfg_.id << ": failed to connect for replication\n";
            }
        }
    }

    // ─── Accept loop ──────────────────────────────────────────────────────────

    void run_accept_loop() {
        while (running_.load() && !global_shutdown().load()) {
            struct sockaddr_in peer_addr{};
            socklen_t peer_len = sizeof(peer_addr);
            int client_fd = accept(listen_fd_,
                                   reinterpret_cast<struct sockaddr*>(&peer_addr),
                                   &peer_len);
            if (client_fd < 0) {
                if (!running_.load()) break;
                continue;
            }

            // Disable Nagle for low latency
            int one = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            // Handle connection in a detached thread
            // (acceptable for a research prototype with limited concurrency)
            std::thread([this, client_fd]() {
                handle_connection(client_fd);
            }).detach();
        }
    }

    // ─── Connection handler ───────────────────────────────────────────────────

    void handle_connection(int fd) {
        std::vector<char> buf;
        std::string line;

        while (running_.load() && net::recv_line(fd, &line, buf)) {
            std::string type = msg::extract_type(line);

            // Apply injected delay if active
            int delay = injected_delay_ms_.load();
            if (delay > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }

            if (type == msg::KV_SET) {
                handle_kv_set(fd, line);
            } else if (type == msg::KV_GET) {
                handle_kv_get(fd, line);
            } else if (type == msg::HEARTBEAT_PING) {
                handle_heartbeat_ping(fd, line);
            } else if (type == msg::REPL_SET) {
                handle_repl_set(fd, line);
            } else if (type == msg::FAULT_DELAY) {
                handle_fault_delay(fd, line);
            }
            // Ignore unknown message types
        }

        net::close_fd(fd);
    }

    // ─── KV handlers ─────────────────────────────────────────────────────────

    void handle_kv_set(int fd, const std::string& line) {
        std::string key = msg::extract_string(line, "key");
        std::string value = msg::extract_string(line, "value");
        std::string req_id = msg::extract_string(line, "req_id");

        logger_.log("client_req_recv", "",
                    "{\"op\":\"SET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) + "\"}");

        // Write to local store
        store_.set(key, value);

        // Replicate if we're primary and have a replicator
        bool repl_ok = true;
        if (cfg_.is_primary && replicator_) {
            repl_ok = replicator_->replicate(key, value);
        }

        // Respond to client
        std::string resp = msg::make_kv_set_resp(key, true, req_id);
        net::send_msg(fd, resp);

        logger_.log("client_req_done", "",
                    "{\"op\":\"SET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) +
                    "\",\"repl_ok\":" + (repl_ok ? "true" : "false") + "}");
    }

    void handle_kv_get(int fd, const std::string& line) {
        std::string key = msg::extract_string(line, "key");
        std::string req_id = msg::extract_string(line, "req_id");

        logger_.log("client_req_recv", "",
                    "{\"op\":\"GET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) + "\"}");

        auto [value, found] = store_.get(key);
        std::string resp = msg::make_kv_get_resp(key, found, value, req_id);
        net::send_msg(fd, resp);

        logger_.log("client_req_done", "",
                    "{\"op\":\"GET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) +
                    "\",\"found\":" + (found ? "true" : "false") + "}");
    }

    // ─── Peer message handlers ────────────────────────────────────────────────

    void handle_heartbeat_ping(int fd, const std::string& line) {
        uint64_t seq = static_cast<uint64_t>(msg::extract_int(line, "seq", 0));
        std::string sender = msg::extract_string(line, "sender_id");
        int64_t ts = wall_ms();
        std::string ack = msg::make_heartbeat_ack(seq, ts, cfg_.id);
        net::send_msg(fd, ack);
    }

    void handle_repl_set(int fd, const std::string& line) {
        std::string key = msg::extract_string(line, "key");
        std::string value = msg::extract_string(line, "value");
        uint64_t seq = static_cast<uint64_t>(msg::extract_int(line, "seq", 0));

        logger_.log("repl_recv", "",
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":" + std::to_string(seq) + "}");

        // Write to local store
        store_.set(key, value);

        // Send ack
        std::string ack = msg::make_repl_set_ack(seq, true);
        net::send_msg(fd, ack);

        logger_.log("repl_ack_sent", "",
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":" + std::to_string(seq) + "}");
    }

    // ─── Fault injection handler ──────────────────────────────────────────────

    void handle_fault_delay(int fd, const std::string& line) {
        int delay = static_cast<int>(msg::extract_int(line, "delay_ms", 0));
        injected_delay_ms_.store(delay);

        logger_.log("fault_injected", "",
                    "{\"type\":\"delay\",\"delay_ms\":" + std::to_string(delay) + "}");

        std::string ack = msg::make_fault_delay_ack(true);
        net::send_msg(fd, ack);
    }

    // ─── Member variables ─────────────────────────────────────────────────────

    NodeConfig cfg_;
    Logger logger_;
    KVStore store_;

    int listen_fd_ = net::INVALID_FD;
    std::atomic<bool> running_;
    std::atomic<int> injected_delay_ms_;

    std::unique_ptr<FailureDetector> fd_;
    std::unique_ptr<Replicator> replicator_;
};

} // namespace kvs
