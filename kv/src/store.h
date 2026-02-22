#pragma once
#include <string>
#include <unordered_map>
#include <optional>
#include <mutex>

class Store {
 public:
  void put(const std::string& k, const std::string& v);
  std::optional<std::string> get(const std::string& k);
  bool del(const std::string& k);

 private:
  std::mutex mu_;
  std::unordered_map<std::string, std::string> kv_;
};