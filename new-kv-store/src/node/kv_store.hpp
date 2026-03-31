/**
 * kv_store.hpp — Thread-safe in-memory key-value store.
 *
 * Simple wrapper around std::unordered_map with mutex protection.
 * Supports SET, GET, and snapshot (for data-loss measurement).
 */

#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kvs {

class KVStore {
public:
    // Set a key-value pair. Overwrites if key exists.
    void set(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mu_);
        data_[key] = value;
    }

    // Get the value for a key.
    // Returns {value, true} if found, {"", false} if not.
    std::pair<std::string, bool> get(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return {it->second, true};
        }
        return {"", false};
    }

    // Return a snapshot of all key-value pairs (for data-loss analysis).
    std::unordered_map<std::string, std::string> snapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return data_;
    }

    // Return the number of stored keys.
    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return data_.size();
    }

    // Return all keys (for data-loss comparison).
    std::vector<std::string> keys() const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::string> result;
        result.reserve(data_.size());
        for (const auto& kv : data_) {
            result.push_back(kv.first);
        }
        return result;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::string> data_;
};

} // namespace kvs
