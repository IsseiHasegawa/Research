#include "node.h"
#include <iostream>
#include <sstream>

static void usage() {
  std::cerr
    << "Usage:\n"
    << "  kvnode --id A --port 8001 --leader 1 --peers B@127.0.0.1:8002,C@127.0.0.1:8003 --log runs/X/A.jsonl\n"
    << "  kvnode --id B --port 8002 --leader 0 --leader_addr 127.0.0.1:8001 --log runs/X/B.jsonl\n"
    << "Options:\n"
    << "  --hb_interval 100   --hb_timeout 500\n";
}

static std::vector<Peer> parse_peers(const std::string& s) {
  std::vector<Peer> peers;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    auto at = item.find('@');
    auto colon = item.rfind(':');
    if (at == std::string::npos || colon == std::string::npos || colon < at) continue;
    Peer p;
    p.id = item.substr(0, at);
    p.host = item.substr(at + 1, colon - (at + 1));
    p.port = std::stoi(item.substr(colon + 1));
    peers.push_back(p);
  }
  return peers;
}

int main(int argc, char** argv) {
  NodeConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.log_path = "node.jsonl";

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&](std::string& out) -> bool {
      if (i + 1 >= argc) return false;
      out = argv[++i];
      return true;
    };

    if (a == "--id") { std::string v; if (!next(v)) return 1; cfg.node_id = v; }
    else if (a == "--port") { std::string v; if (!next(v)) return 1; cfg.port = std::stoi(v); }
    else if (a == "--leader") { std::string v; if (!next(v)) return 1; cfg.is_leader = (v == "1"); }
    else if (a == "--leader_addr") {
      std::string v; if (!next(v)) return 1;
      auto colon = v.rfind(':');
      if (colon == std::string::npos) return 1;
      cfg.leader_host = v.substr(0, colon);
      cfg.leader_port = std::stoi(v.substr(colon + 1));
    }
    else if (a == "--peers") { std::string v; if (!next(v)) return 1; cfg.peers = parse_peers(v); }
    else if (a == "--log") { std::string v; if (!next(v)) return 1; cfg.log_path = v; }
    else if (a == "--hb_interval") { std::string v; if (!next(v)) return 1; cfg.heartbeat_interval_ms = std::stoi(v); }
    else if (a == "--hb_timeout") { std::string v; if (!next(v)) return 1; cfg.heartbeat_timeout_ms = std::stoi(v); }
    else if (a == "--help") { usage(); return 0; }
  }

  if (cfg.node_id.empty() || cfg.port == 0) {
    usage();
    return 1;
  }

  Node n(cfg);
  n.run();
  return 0;
}