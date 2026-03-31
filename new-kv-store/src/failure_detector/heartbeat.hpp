/**
 * heartbeat.hpp — Heartbeat-based failure detection.
 *
 * Components:
 *   - HeartbeatSender:  periodically sends HEARTBEAT_PING to a peer
 *   - HeartbeatResponder: replies to received pings with HEARTBEAT_ACK
 *   - FailureDetector: monitors last-ack time and declares peer dead
 *
 * Timing:
 *   - Internal timeout math uses monotonic clock (immune to NTP jumps)
 *   - Log timestamps use wall clock (for cross-process alignment)
 *
 * The failure detector runs on a dedicated checker thread that polls at
 * a configurable check interval (default 10ms). Once a peer is declared
 * dead, the declaration is logged exactly once and the dead flag is set.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>

#include "../common/logger.hpp"
#include "../common/message.hpp"
#include "../common/net.hpp"

namespace kvs {

/**
 * FailureDetector — runs the heartbeat sender, receiver (ACK reader), and
 * checker loop for one peer connection.
 *
 * Usage:
 *   1. Construct with parameters
 *   2. Call start() with the connected socket fd to the peer
 *   3. The detector runs until the peer is declared dead or stop() is called
 *   4. Query is_peer_dead() to check status
 */
class FailureDetector {
public:
    FailureDetector(const std::string& my_id,
                    const std::string& peer_id,
                    int hb_interval_ms,
                    int hb_timeout_ms,
                    Logger& logger)
        : my_id_(my_id)
        , peer_id_(peer_id)
        , hb_interval_ms_(hb_interval_ms)
        , hb_timeout_ms_(hb_timeout_ms)
        , logger_(logger)
        , running_(false)
        , dead_declared_(false)
        , last_ack_mono_(0)
    {}

    // Start the failure detector threads on the given peer socket.
    // The socket must already be connected to the peer's heartbeat port.
    void start(int peer_fd) {
        peer_fd_ = peer_fd;
        running_.store(true);
        last_ack_mono_.store(monotonic_ms());
        start_wall_ = wall_ms();

        // Sender thread: sends heartbeat pings at hb_interval_ms
        sender_thread_ = std::thread([this]() { run_sender(); });

        // Receiver thread: reads heartbeat acks and updates last_ack_mono_
        receiver_thread_ = std::thread([this]() { run_receiver(); });

        // Checker thread: declares peer dead if timeout exceeded
        checker_thread_ = std::thread([this]() { run_checker(); });
    }

    // Stop all threads gracefully.
    void stop() {
        running_.store(false);
        if (sender_thread_.joinable()) sender_thread_.join();
        if (receiver_thread_.joinable()) receiver_thread_.join();
        if (checker_thread_.joinable()) checker_thread_.join();
    }

    // Has this detector declared the peer as dead?
    bool is_peer_dead() const { return dead_declared_.load(); }

    // Set callback for when peer is declared dead.
    void on_peer_dead(std::function<void()> cb) { on_dead_cb_ = std::move(cb); }

private:
    void run_sender() {
        uint64_t seq = 0;
        while (running_.load() && !dead_declared_.load()) {
            int64_t ts = wall_ms();
            std::string ping = msg::make_heartbeat_ping(seq++, ts, my_id_);
            if (!net::send_msg(peer_fd_, ping)) {
                // Send failed — peer socket probably closed. The checker
                // thread will eventually declare dead via timeout.
                break;
            }
            logger_.log("hb_ping_sent", peer_id_);

            // Sleep in small increments so we can check stop flag
            for (int elapsed = 0;
                 elapsed < hb_interval_ms_ && running_.load() && !dead_declared_.load();
                 elapsed += 10) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    void run_receiver() {
        // Non-blocking reads with select-based timeout
        net::set_nonblocking(peer_fd_, true);
        std::vector<char> buf;
        std::string line;
        int wait_ms = hb_timeout_ms_ + 50;

        while (running_.load()) {
            if (!net::recv_line(peer_fd_, &line, buf, true, wait_ms)) {
                // EOF or error — peer closed. Stop receiving.
                break;
            }
            if (dead_declared_.load()) continue;  // Ignore late acks

            std::string type = msg::extract_type(line);
            if (type == msg::HEARTBEAT_ACK) {
                last_ack_mono_.store(monotonic_ms());
                logger_.log("hb_ack_recv", peer_id_);
            }
        }
    }

    void run_checker() {
        const int check_interval_ms = 10;
        // Grace period: don't declare dead during the first 2 seconds
        // (allows connection setup and first heartbeat exchanges)
        const int64_t grace_ms = 2000;

        while (running_.load() && !dead_declared_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));

            int64_t now_mono = monotonic_ms();
            int64_t now_wall = wall_ms();

            // Skip during grace period
            if (now_wall - start_wall_ < grace_ms) continue;

            int64_t last = last_ack_mono_.load();
            if (now_mono - last >= hb_timeout_ms_) {
                dead_declared_.store(true);
                logger_.log("declared_dead", peer_id_);
                if (on_dead_cb_) on_dead_cb_();
                break;
            }
        }
    }

    std::string my_id_;
    std::string peer_id_;
    int hb_interval_ms_;
    int hb_timeout_ms_;
    Logger& logger_;

    int peer_fd_ = net::INVALID_FD;
    std::atomic<bool> running_;
    std::atomic<bool> dead_declared_;
    std::atomic<int64_t> last_ack_mono_;
    int64_t start_wall_ = 0;

    std::thread sender_thread_;
    std::thread receiver_thread_;
    std::thread checker_thread_;

    std::function<void()> on_dead_cb_;
};

} // namespace kvs
