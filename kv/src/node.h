#pragma once
#include "store.h"
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <mutex>

struct Peer {
  std::string id;
  std::string host;
  int port;
};

struct NodeConfig {
  std::string node_id;
  std::string host = "127.0.0.1";
  int port = 0;

  bool is_leader = false;
  std::string leader_host = "127.0.0.1";
  int leader_port = 0;

  std::vector<Peer> peers;

  int heartbeat_interval_ms = 100;
  int heartbeat_timeout_ms  = 500;

  std::string log_path = "node.jsonl";
};

class Node {
 public:
  explicit Node(NodeConfig cfg);
  void run();   // blocking

 private:
  NodeConfig cfg_;
  Store store_;

  std::atomic<bool> running_{false};
  std::thread hb_thread_;

  std::mutex fd_mu_;
  std::unordered_map<std::string, int64_t> last_ok_ms_;
  std::unordered_map<std::string, std::string> fd_state_;

  std::atomic<int64_t> op_seq_{0};

  void start_heartbeat_loop();

  void log_event(const std::string& type,
                 const std::string& request_id,
                 const std::string& key,
                 const std::string& extra_json);

  void replicate_async(const std::string& request_id,
                       const std::string& op,
                       const std::string& key,
                       const std::string& value);

  void fd_update_peer(const std::string& peer_id, bool ok, int64_t t);
  bool leader_is_dead(int64_t now);
};