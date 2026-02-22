#include "util.h"
#include <chrono>
#include <fstream>
#include <mutex>
#include <ctime>

static std::mutex g_log_mu;

int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string iso_time() {
  std::time_t t = std::time(nullptr);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[64];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return std::string(buf);
}

void append_jsonl(const std::string& path, const std::string& line) {
  std::lock_guard<std::mutex> lk(g_log_mu);
  std::ofstream out(path, std::ios::app);
  out << line << "\n";
}