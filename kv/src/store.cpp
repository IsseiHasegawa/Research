#include "store.h"

void Store::put(const std::string& k, const std::string& v) {
  std::lock_guard<std::mutex> lk(mu_);
  kv_[k] = v;
}

std::optional<std::string> Store::get(const std::string& k) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = kv_.find(k);
  if (it == kv_.end()) return std::nullopt;
  return it->second;
}

bool Store::del(const std::string& k) {
  std::lock_guard<std::mutex> lk(mu_);
  return kv_.erase(k) > 0;
}