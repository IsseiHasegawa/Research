/**
 * Minimal 2-node KV store with heartbeat failure detection.
 * Single binary: --role detector (A) or monitored (B).
 * SPEC: kv-2node-fd-spec/SPEC.md
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#define INVALID_SOCKET (-1)
#define SOCKET_ERROR   (-1)
typedef int SOCKET;
#endif

// --- Time (wall for logs, monotonic for timeout) ---
static int64_t wall_ms() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

static int64_t monotonic_ms() {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

// --- JSON escape for log values ---
static std::string json_escape(const std::string& s) {
  std::ostringstream out;
  for (char c : s) {
    if (c == '"') out << "\\\"";
    else if (c == '\\') out << "\\\\";
    else if (c == '\n') out << "\\n";
    else if (c == '\r') out << "\\r";
    else if (static_cast<unsigned char>(c) < 32) {
      out << "\\u00" << std::hex << std::setw(2) << std::setfill('0') << (unsigned)(unsigned char)c << std::dec;
    }
    else out << c;
  }
  return out.str();
}

// --- Logger (JSONL, wall clock ts_ms) ---
struct Logger {
  std::ofstream f;
  std::mutex m;
  std::string node_id;
  std::string run_id;
  int hb_interval_ms = 0;
  int hb_timeout_ms  = 0;

  bool open(const std::string& path) {
    f.open(path, std::ios::out | std::ios::app);
    return f.is_open();
  }

  void log(const std::string& event, const std::string& peer_id,
           const std::string& extra_json = "{}") {
    std::lock_guard<std::mutex> lock(m);
    if (!f.is_open()) return;
    int64_t ts = wall_ms();
    f << "{\"ts_ms\":" << ts << ",\"node_id\":\"" << json_escape(node_id)
      << "\",\"run_id\":\"" << json_escape(run_id) << "\",\"hb_interval_ms\":"
      << hb_interval_ms << ",\"hb_timeout_ms\":" << hb_timeout_ms
      << ",\"event\":\"" << json_escape(event) << "\"";
    if (!peer_id.empty())
      f << ",\"peer_id\":\"" << json_escape(peer_id) << "\"";
    else
      f << ",\"peer_id\":null";
    f << ",\"extra\":" << extra_json << "}\n";
    f.flush();  // so injector can read declared_dead immediately
  }
};

// --- Minimal JSON type extraction (find "type":"XXX") ---
static std::string extract_type(const std::string& line) {
  const std::string key = "\"type\":\"";
  auto i = line.find(key);
  if (i == std::string::npos) return "";
  i += key.size();
  auto j = line.find('"', i);
  if (j == std::string::npos) return "";
  return line.substr(i, j - i);
}

// Extract string value for key (e.g. "key":"val")
static std::string extract_string(const std::string& line, const std::string& key_name) {
  std::string key = "\"" + key_name + "\":\"";
  auto i = line.find(key);
  if (i == std::string::npos) return "";
  i += key.size();
  auto j = i;
  while (j < line.size() && line[j] != '"') {
    if (line[j] == '\\' && j + 1 < line.size()) j++;
    j++;
  }
  if (j >= line.size()) return "";
  std::string val;
  for (size_t k = i; k < j; k++) {
    if (line[k] == '\\' && k + 1 < j) { k++; val += line[k]; continue; }
    val += line[k];
  }
  return val;
}

// --- Socket helpers (POSIX-style; Windows would need different) ---
#ifndef _WIN32
static void close_socket(SOCKET s) { if (s >= 0) close(s); }
#else
static void close_socket(SOCKET s) { if (s != INVALID_SOCKET) closesocket(s); }
#endif

static bool set_nonblocking(SOCKET s, bool nonblock) {
#ifndef _WIN32
  int flags = fcntl(s, F_GETFL, 0);
  if (flags < 0) return false;
  if (nonblock) flags |= O_NONBLOCK; else flags &= ~O_NONBLOCK;
  return fcntl(s, F_SETFL, flags) == 0;
#else
  u_long mode = nonblock ? 1 : 0;
  return ioctlsocket(s, FIONBIO, &mode) == 0;
#endif
}

// Read one line (ending with \n) into *out; returns true when a full line is read.
static bool read_line(SOCKET fd, std::string* out, std::vector<char>& buf) {
  out->clear();
  for (;;) {
    auto it = std::find(buf.begin(), buf.end(), '\n');
    if (it != buf.end()) {
      out->assign(buf.begin(), it);
      buf.erase(buf.begin(), it + 1);
      return true;
    }
    if (buf.size() > 1024 * 1024) return false;  // sanity
    char b[256];
    int n = recv(fd, b, sizeof(b), 0);
    if (n <= 0) return false;
    buf.insert(buf.end(), b, b + n);
  }
}

static bool send_all(SOCKET fd, const char* data, size_t len) {
  while (len > 0) {
    int n = send(fd, data, static_cast<int>(len), 0);
    if (n <= 0) return false;
    data += n;
    len -= n;
  }
  return true;
}

// --- KV store + protocol response builders ---
static std::string kv_get_resp(const std::string& key, bool ok, const std::string& value) {
  std::ostringstream o;
  o << "{\"type\":\"KV_GET_RESP\",\"key\":\"" << json_escape(key) << "\",\"value\":";
  if (value.empty() && !ok) o << "null";
  else o << "\"" << json_escape(value) << "\"";
  o << ",\"ok\":" << (ok ? "true" : "false") << "}\n";
  return o.str();
}

static std::string kv_set_resp(const std::string& key, bool ok) {
  return "{\"type\":\"KV_SET_RESP\",\"key\":\"" + json_escape(key) + "\",\"ok\":" + (ok ? "true" : "false") + "}\n";
}

// --- Monitored node (B): listen, handle PING->ACK and KV ---
static void run_monitored(Logger& log, int port, int hb_interval_ms, int hb_timeout_ms,
                          const std::string& run_id) {
  log.run_id = run_id;
  log.hb_interval_ms = hb_interval_ms;
  log.hb_timeout_ms  = hb_timeout_ms;

  SOCKET listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd == INVALID_SOCKET) {
    std::cerr << "socket failed\n";
    return;
  }
  int one = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), sizeof(one));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::cerr << "bind failed\n";
    close_socket(listen_fd);
    return;
  }
  if (listen(listen_fd, 5) != 0) {
    std::cerr << "listen failed\n";
    close_socket(listen_fd);
    return;
  }
  std::cerr << "B listening on port " << port << std::endl;

  std::map<std::string, std::string> kv;

  while (true) {
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    SOCKET client = accept(listen_fd, reinterpret_cast<struct sockaddr*>(&peer), &peer_len);
    if (client == INVALID_SOCKET) continue;
    std::thread([client, &log, &kv]() {
      std::vector<char> buf;
      std::string line;
      while (read_line(client, &line, buf)) {
        std::string type = extract_type(line);
        if (type == "HEARTBEAT_PING") {
          int64_t ts = wall_ms();
          std::string ack = "{\"type\":\"HEARTBEAT_ACK\",\"ts_ms\":" + std::to_string(ts) + "}\n";
          send_all(client, ack.data(), ack.size());
        } else if (type == "KV_GET") {
          std::string key = extract_string(line, "key");
          auto it = kv.find(key);
          bool ok = (it != kv.end());
          std::string val = ok ? it->second : "";
          log.log("kv_get", "", "{\"key\":\"" + json_escape(key) + "\"}");
          std::string resp = kv_get_resp(key, ok, val);
          send_all(client, resp.data(), resp.size());
          log.log("kv_resp", "", "{\"key\":\"" + json_escape(key) + "\",\"ok\":" + (ok ? "true" : "false") + ",\"value\":\"" + json_escape(val) + "\"}");
        } else if (type == "KV_SET") {
          std::string key = extract_string(line, "key");
          std::string val = extract_string(line, "value");
          kv[key] = val;
          log.log("kv_set", "", "{\"key\":\"" + json_escape(key) + "\"}");
          std::string resp = kv_set_resp(key, true);
          send_all(client, resp.data(), resp.size());
        }
      }
      close_socket(client);
    }).detach();
  }
  close_socket(listen_fd);
}

// --- Detector (A): KV server + heartbeat client to B, FD loop ---
static void run_detector(Logger& log, int port, const std::string& peer_addr,
                         int hb_interval_ms, int hb_timeout_ms, const std::string& run_id) {
  log.run_id = run_id;
  log.hb_interval_ms = hb_interval_ms;
  log.hb_timeout_ms  = hb_timeout_ms;
  std::string peer_id = "B";

  // Parse peer_addr "host:port"
  std::string host = "127.0.0.1";
  int peer_port = 8002;
  size_t colon = peer_addr.find(':');
  if (colon != std::string::npos) {
    host = peer_addr.substr(0, colon);
    peer_port = std::stoi(peer_addr.substr(colon + 1));
  } else {
    peer_port = std::stoi(peer_addr);
  }

  std::atomic<int64_t> last_ack_time_mono{monotonic_ms()};
  std::atomic<bool> dead_declared{false};

  // KV server thread (same as B, but no HEARTBEAT_PING handling)
  std::map<std::string, std::string> kv;
  SOCKET listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd == INVALID_SOCKET) { std::cerr << "socket failed\n"; return; }
  int one = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), sizeof(one));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::cerr << "bind failed\n"; close_socket(listen_fd); return;
  }
  if (listen(listen_fd, 5) != 0) {
    std::cerr << "listen failed\n"; close_socket(listen_fd); return;
  }
  std::cerr << "A listening on port " << port << ", peer " << peer_addr << std::endl;

  std::thread kv_server([listen_fd, &log, &kv]() {
    while (true) {
      struct sockaddr_in peer;
      socklen_t peer_len = sizeof(peer);
      SOCKET client = accept(listen_fd, reinterpret_cast<struct sockaddr*>(&peer), &peer_len);
      if (client == INVALID_SOCKET) break;
      std::thread([client, &log, &kv]() {
        std::vector<char> buf;
        std::string line;
        while (read_line(client, &line, buf)) {
          std::string type = extract_type(line);
          if (type == "KV_GET") {
            std::string key = extract_string(line, "key");
            auto it = kv.find(key);
            bool ok = (it != kv.end());
            std::string val = ok ? it->second : "";
            log.log("kv_get", "", "{\"key\":\"" + json_escape(key) + "\"}");
            std::string resp = kv_get_resp(key, ok, val);
            send_all(client, resp.data(), resp.size());
            log.log("kv_resp", "", "{\"key\":\"" + json_escape(key) + "\",\"ok\":" + (ok ? "true" : "false") + ",\"value\":\"" + json_escape(val) + "\"}");
          } else if (type == "KV_SET") {
            std::string key = extract_string(line, "key");
            std::string val = extract_string(line, "value");
            kv[key] = val;
            log.log("kv_set", "", "{\"key\":\"" + json_escape(key) + "\"}");
            std::string resp = kv_set_resp(key, true);
            send_all(client, resp.data(), resp.size());
          }
        }
        close_socket(client);
      }).detach();
    }
    close_socket(listen_fd);
  });

  // Connect to B
  struct sockaddr_in peer_addr_in;
  memset(&peer_addr_in, 0, sizeof(peer_addr_in));
  peer_addr_in.sin_family = AF_INET;
  peer_addr_in.sin_port = htons(static_cast<uint16_t>(peer_port));
#ifdef _WIN32
  InetPtonA(AF_INET, host.c_str(), &peer_addr_in.sin_addr);
#else
  inet_pton(AF_INET, host.c_str(), &peer_addr_in.sin_addr);
#endif
  SOCKET hb_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (hb_fd == INVALID_SOCKET) {
    std::cerr << "A: socket failed\n";
    return;
  }
  if (connect(hb_fd, reinterpret_cast<struct sockaddr*>(&peer_addr_in), sizeof(peer_addr_in)) != 0) {
    std::cerr << "A: connect to " << peer_addr << " failed\n";
    close_socket(hb_fd);
    return;
  }

  // No SO_RCVTIMEO: use blocking reads so recv() returns only on data or connection close/error.
  // This avoids treating receive timeout as connection loss when B is alive but slow.

  // Sender: every hb_interval_ms send PING and log
  std::thread sender([hb_fd, &log, &dead_declared, hb_interval_ms, peer_id]() {
    uint64_t seq = 0;
    while (!dead_declared.load()) {
      int64_t ts = wall_ms();
      std::string ping = "{\"type\":\"HEARTBEAT_PING\",\"seq\":" + std::to_string(seq++) + ",\"ts_ms\":" + std::to_string(ts) + "}\n";
      if (!send_all(hb_fd, ping.data(), ping.size())) break;
      log.log("hb_ping_sent", peer_id, "{}");
      for (int i = 0; i < hb_interval_ms && !dead_declared.load(); i += 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  });

  // Receiver: read ACKs, update last_ack_time and log (ignore ACKs after dead_declared per spec)
  std::thread receiver([hb_fd, &log, &last_ack_time_mono, &dead_declared, peer_id]() {
    std::vector<char> buf;
    std::string line;
    while (read_line(hb_fd, &line, buf)) {
      if (dead_declared.load()) continue;  // ignore late ACKs
      if (extract_type(line) == "HEARTBEAT_ACK") {
        last_ack_time_mono.store(monotonic_ms());
        log.log("hb_ack_recv", peer_id, "{}");
      }
    }
  });

  // Checker: every 10ms, if now - last_ack_time >= hb_timeout_ms then declare dead once
  const int check_interval_ms = 10;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));
    if (dead_declared.load()) continue;
    int64_t now = monotonic_ms();
    int64_t last = last_ack_time_mono.load();
    int64_t delta = now - last;
    if (delta >= hb_timeout_ms) {
      dead_declared.store(true);
      log.log("declared_dead", peer_id, "{}");
      break;
    }
  }

  close_socket(hb_fd);  // unblock receiver
  sender.join();
  receiver.join();
  // Keep process alive so injector can read log; KV server runs in background
  while (true) std::this_thread::sleep_for(std::chrono::hours(1));
}

// --- CLI ---
static void usage(const char* prog) {
  std::cerr << "Usage: " << prog
            << " --id <A|B> --port <port> --role <detector|monitored> --log_path <path>\n"
            << "       --hb_interval_ms <ms> --hb_timeout_ms <ms>\n"
            << "       [--peer_addr <host:port>]  (required if role=detector)\n"
            << "       [--run_id <id>]\n";
}

int main(int argc, char* argv[]) {
#ifdef _WIN32
  WSADATA wsa;
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return 1;
#endif

  std::string id, role, log_path, peer_addr, run_id;
  int port = 0, hb_interval_ms = 0, hb_timeout_ms = 0;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--id" && i + 1 < argc) { id = argv[++i]; continue; }
    if (arg == "--port" && i + 1 < argc) { port = std::stoi(argv[++i]); continue; }
    if (arg == "--role" && i + 1 < argc) { role = argv[++i]; continue; }
    if (arg == "--log_path" && i + 1 < argc) { log_path = argv[++i]; continue; }
    if (arg == "--hb_interval_ms" && i + 1 < argc) { hb_interval_ms = std::stoi(argv[++i]); continue; }
    if (arg == "--hb_timeout_ms" && i + 1 < argc) { hb_timeout_ms = std::stoi(argv[++i]); continue; }
    if (arg == "--peer_addr" && i + 1 < argc) { peer_addr = argv[++i]; continue; }
    if (arg == "--run_id" && i + 1 < argc) { run_id = argv[++i]; continue; }
  }

  const char* run_id_env = std::getenv("RUN_ID");
  if (run_id.empty() && run_id_env) run_id = run_id_env;
  if (run_id.empty()) run_id = "default_run";

  if (id.empty() || port <= 0 || role.empty() || log_path.empty() ||
      hb_interval_ms <= 0 || hb_timeout_ms <= 0) {
    usage(argv[0]);
    return 1;
  }
  if (role == "detector" && peer_addr.empty()) {
    std::cerr << "detector requires --peer_addr\n";
    usage(argv[0]);
    return 1;
  }

  Logger log;
  log.node_id = id;
  if (!log.open(log_path)) {
    std::cerr << "Failed to open log " << log_path << "\n";
    return 1;
  }

  if (role == "monitored") {
    run_monitored(log, port, hb_interval_ms, hb_timeout_ms, run_id);
  } else if (role == "detector") {
    run_detector(log, port, peer_addr, hb_interval_ms, hb_timeout_ms, run_id);
  } else {
    std::cerr << "role must be detector or monitored\n";
    return 1;
  }

#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}
