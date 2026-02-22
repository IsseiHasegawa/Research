// src/node.cpp
#include "node.h"
#include "util.h"

#include "httplib.h"
#include <nlohmann/json.hpp>

#include <iostream>

using json = nlohmann::json;

static std::string make_rid() {
  return std::to_string(now_ms()) + "-" + std::to_string(reinterpret_cast<uintptr_t>(&make_rid));
}

Node::Node(NodeConfig cfg) : cfg_(std::move(cfg)) {}

void Node::log_event(const std::string& type,
                     const std::string& request_id,
                     const std::string& key,
                     const std::string& extra_json) {
  json j;
  j["ts_ms"] = now_ms();
  j["ts_iso"] = iso_time();
  j["node_id"] = cfg_.node_id;
  j["type"] = type;
  if (!request_id.empty()) j["rid"] = request_id;
  if (!key.empty()) j["key"] = key;
  j["seq"] = op_seq_.load();

  json extra = json::parse(extra_json, nullptr, false);
  if (!extra.is_discarded() && extra.is_object()) {
    for (auto& [k, v] : extra.items()) j[k] = v;
  }
  append_jsonl(cfg_.log_path, j.dump());
}

void Node::fd_update_peer(const std::string& peer_id, bool ok, int64_t t) {
  std::lock_guard<std::mutex> lk(fd_mu_);
  if (ok) last_ok_ms_[peer_id] = t;

  int64_t last_ok = 0;
  if (auto it = last_ok_ms_.find(peer_id); it != last_ok_ms_.end()) last_ok = it->second;

  std::string prev = "Alive";
  if (auto it = fd_state_.find(peer_id); it != fd_state_.end()) prev = it->second;

  std::string next = prev;
  if (ok) next = "Alive";
  else {
    // Never declare dead if we've never seen a successful heartbeat (avoids false positive at startup)
    if (last_ok == 0) next = "Suspected";
    else {
      int64_t diff = t - last_ok;
      if (diff > cfg_.heartbeat_timeout_ms) next = "Dead";
      else next = "Suspected";
    }
  }

  if (next != prev) {
    fd_state_[peer_id] = next;
    log_event("fd_state_change", "", "",
              json({{"peer_id", peer_id}, {"from", prev}, {"to", next}}).dump());
  } else {
    fd_state_[peer_id] = prev;
  }
}

bool Node::leader_is_dead(int64_t now) {
  std::lock_guard<std::mutex> lk(fd_mu_);
  int64_t last = 0;
  if (auto it = last_ok_ms_.find("leader"); it != last_ok_ms_.end()) last = it->second;
  if (last == 0) return false;  // Never seen success → don't declare dead (avoids startup false positive)
  return (now - last) > cfg_.heartbeat_timeout_ms;
}

void Node::replicate_async(const std::string& request_id,
                           const std::string& op,
                           const std::string& key,
                           const std::string& value) {
  if (!cfg_.is_leader) return;

  json body;
  body["rid"] = request_id;
  body["op"] = op;
  body["key"] = key;
  body["value"] = value;

  for (const auto& p : cfg_.peers) {
    std::thread([this, p, body, request_id, key]() {
      httplib::Client cli(p.host, p.port);
      cli.set_connection_timeout(0, 200000);
      cli.set_read_timeout(0, 500000);

      auto res = cli.Post("/internal/replicate", body.dump(), "application/json");
      int64_t t = now_ms();
      bool ok = (res && res->status == 200);

      fd_update_peer(p.id, ok, t);
      log_event("replicate_result", request_id, key,
                json({{"peer_id", p.id}, {"ok", ok}, {"http_status", res ? res->status : 0}}).dump());
    }).detach();
  }
}

void Node::start_heartbeat_loop() {
  hb_thread_ = std::thread([this]() {
    while (running_) {
      int64_t t0 = now_ms();

      if (cfg_.is_leader) {
        for (const auto& p : cfg_.peers) {
          httplib::Client cli(p.host, p.port);
          cli.set_connection_timeout(0, 200000);
          cli.set_read_timeout(0, 200000);
          auto res = cli.Get("/internal/ping");
          bool ok = (res && res->status == 200);
          fd_update_peer(p.id, ok, now_ms());
        }
      } else {
        httplib::Client cli(cfg_.leader_host, cfg_.leader_port);
        cli.set_connection_timeout(0, 200000);
        cli.set_read_timeout(0, 200000);
        auto res = cli.Get(("/internal/ping?from=" + cfg_.node_id).c_str());
        bool ok = (res && res->status == 200);
        int64_t t1 = now_ms();
        fd_update_peer("leader", ok, t1);
      }

      int64_t spent = now_ms() - t0;
      int64_t sleep_ms = cfg_.heartbeat_interval_ms - spent;
      if (sleep_ms < 1) sleep_ms = 1;
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
  });
}

void Node::run() {
  running_ = true;
  start_heartbeat_loop();

  httplib::Server svr;

  svr.Post("/put", [&](const httplib::Request& req, httplib::Response& resp) {
    auto rid = req.has_param("rid") ? req.get_param_value("rid") : make_rid();
    op_seq_++;

    json body = json::parse(req.body, nullptr, false);
    if (body.is_discarded() || !body.contains("key") || !body.contains("value")) {
      resp.status = 400;
      resp.set_content(R"({"error":"bad_json"})", "application/json");
      log_event("put_badreq", rid, "", "{}");
      return;
    }

    std::string key = body["key"].get<std::string>();
    std::string val = body["value"].get<std::string>();

    if (!cfg_.is_leader) {
      resp.status = 409;
      resp.set_content(R"({"error":"not_leader"})", "application/json");
      log_event("put_reject_not_leader", rid, key, "{}");
      return;
    }

    store_.put(key, val);
    log_event("put_ok", rid, key, json({{"value_len", val.size()}}).dump());
    replicate_async(rid, "PUT", key, val);

    resp.status = 200;
    resp.set_content(json({{"ok", true}, {"rid", rid}}).dump(), "application/json");
  });

  svr.Post("/get", [&](const httplib::Request& req, httplib::Response& resp) {
    auto rid = req.has_param("rid") ? req.get_param_value("rid") : make_rid();
    op_seq_++;

    json body = json::parse(req.body, nullptr, false);
    if (body.is_discarded() || !body.contains("key")) {
      resp.status = 400;
      resp.set_content(R"({"error":"bad_json"})", "application/json");
      log_event("get_badreq", rid, "", "{}");
      return;
    }

    std::string key = body["key"].get<std::string>();
    auto v = store_.get(key);

    if (!v) {
      resp.status = 404;
      resp.set_content(json({{"ok", false}, {"rid", rid}, {"found", false}}).dump(), "application/json");
      log_event("get_notfound", rid, key, "{}");
      return;
    }

    resp.status = 200;
    resp.set_content(json({{"ok", true}, {"rid", rid}, {"found", true}, {"value", *v}}).dump(), "application/json");
    log_event("get_ok", rid, key, json({{"value_len", v->size()}}).dump());
  });

  svr.Get("/internal/ping", [&](const httplib::Request&, httplib::Response& resp) {
    resp.status = 200;
    resp.set_content(R"({"ok":true})", "application/json");
  });

  svr.Post("/internal/replicate", [&](const httplib::Request& req, httplib::Response& resp) {
    json body = json::parse(req.body, nullptr, false);
    if (body.is_discarded() || !body.contains("op") || !body.contains("key") || !body.contains("rid")) {
      resp.status = 400;
      resp.set_content(R"({"error":"bad_json"})", "application/json");
      return;
    }

    std::string op = body["op"].get<std::string>();
    std::string key = body["key"].get<std::string>();
    std::string rid = body["rid"].get<std::string>();
    std::string value = body.value("value", "");

    if (op == "PUT") store_.put(key, value);
    else if (op == "DEL") store_.del(key);

    log_event("replicate_apply", rid, key, json({{"op", op}}).dump());
    resp.status = 200;
    resp.set_content(R"({"ok":true})", "application/json");
  });

  log_event("node_start", "", "", json({{"host", cfg_.host}, {"port", cfg_.port}, {"is_leader", cfg_.is_leader}}).dump());
  std::cout << "Node " << cfg_.node_id << " listening on " << cfg_.host << ":" << cfg_.port
            << (cfg_.is_leader ? " (leader)" : " (follower)") << "\n";

  svr.listen(cfg_.host.c_str(), cfg_.port);

  log_event("node_stop", "", "", "{}");
  running_ = false;
  if (hb_thread_.joinable()) hb_thread_.join();
}