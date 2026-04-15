/**
 * logger.hpp — Thread-safe JSONL structured logger.
 *
 * Writes one JSON object per line to a file, with fields:
 *   ts_ms, node_id, run_id, event, peer_id (optional), extra (optional JSON object)
 *
 * All timestamps use wall clock (ms since epoch) for cross-process alignment.
 * The logger flushes after every write so that external log readers (e.g. the
 * Python injector) can see events immediately after they are recorded.
 */

#pragma once

#include <chrono>
#include <fstream>
#include <mutex>
#include <string>

#include "message.hpp"  // for json_escape

namespace kvs {

// Wall clock timestamp in milliseconds since epoch.
inline int64_t wall_ms() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch())
        .count();
}

// Monotonic clock timestamp in milliseconds (for internal timeout math).
inline int64_t monotonic_ms() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch())
        .count();
}

class Logger {
public:
    Logger() = default;

    // Open the log file (truncate). Each process start gets a fresh file so
    // experiment trials and metrics are not polluted by prior runs on the same path.
    bool open(const std::string& path) {
        std::lock_guard<std::mutex> lock(mu_);
        file_.open(path, std::ios::out | std::ios::trunc);
        return file_.is_open();
    }

    // Set metadata fields that appear in every log line.
    void set_metadata(const std::string& node_id, const std::string& run_id) {
        std::lock_guard<std::mutex> lock(mu_);
        node_id_ = node_id;
        run_id_ = run_id;
    }

    /**
     * Log a structured event.
     *
     * @param event     Event type string (e.g. "hb_ping_sent", "declared_dead")
     * @param peer_id   Related peer node ID, or "" for none (logged as null)
     * @param extra     Additional JSON object string, or "{}" for empty
     */
    void log(const std::string& event, const std::string& peer_id = "",
             const std::string& extra = "{}") {
        std::lock_guard<std::mutex> lock(mu_);
        if (!file_.is_open()) return;

        int64_t ts = wall_ms();
        file_ << "{\"ts_ms\":" << ts
              << ",\"node_id\":\"" << msg::json_escape(node_id_)
              << "\",\"run_id\":\"" << msg::json_escape(run_id_)
              << "\",\"event\":\"" << msg::json_escape(event) << "\"";

        if (!peer_id.empty()) {
            file_ << ",\"peer_id\":\"" << msg::json_escape(peer_id) << "\"";
        } else {
            file_ << ",\"peer_id\":null";
        }

        file_ << ",\"extra\":" << extra << "}\n";
        file_.flush();  // Ensure external readers see this immediately
    }

    bool is_open() const {
        // Not strictly thread-safe for the check, but sufficient for our use.
        return file_.is_open();
    }

private:
    std::ofstream file_;
    std::mutex mu_;
    std::string node_id_;
    std::string run_id_;
};

} // namespace kvs
