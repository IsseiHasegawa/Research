/**
 * main.cpp — Entry point for the distributed KV store node.
 *
 * Parses CLI arguments, creates a Node, and runs it.
 * Handles SIGINT/SIGTERM for clean shutdown logging.
 *
 * Usage:
 *   ./kvnode --id node0 --port 9100 --primary \
 *            --peer 127.0.0.1:9101 \
 *            --log_path output/logs/node0.jsonl \
 *            --run_id experiment_001 \
 *            --hb_interval_ms 100 --hb_timeout_ms 400 \
 *            --repl_mode sync
 *
 *   ./kvnode --id node1 --port 9101 \
 *            --log_path output/logs/node1.jsonl \
 *            --run_id experiment_001 \
 *            --hb_interval_ms 100 --hb_timeout_ms 400
 */

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

#include "node/node.hpp"

// Global node pointer for signal handler
static kvs::Node* g_node = nullptr;

static void signal_handler(int sig) {
    (void)sig;
    kvs::Node::global_shutdown().store(true);
    if (g_node) g_node->stop();
}

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "\n"
        << "Required:\n"
        << "  --id <name>           Node identifier (e.g. node0, node1)\n"
        << "  --port <port>         TCP port to listen on\n"
        << "  --log_path <path>     Path to JSONL log file\n"
        << "\n"
        << "Optional:\n"
        << "  --primary             Mark this node as the primary (initiates FD + repl)\n"
        << "  --peer <host:port>    Peer node address (required if --primary)\n"
        << "  --run_id <id>         Experiment run identifier (default: from $RUN_ID or \"default_run\")\n"
        << "  --hb_interval_ms <ms> Heartbeat interval in ms (default: 100)\n"
        << "  --hb_timeout_ms <ms>  Heartbeat timeout in ms (default: 400)\n"
        << "  --repl_mode <mode>    Replication mode: none|sync|async (default: none)\n"
        << "\n"
        << "Example:\n"
        << "  " << prog << " --id node0 --port 9100 --primary --peer 127.0.0.1:9101 "
        << "--log_path logs/n0.jsonl --repl_mode sync\n";
}

int main(int argc, char* argv[]) {
    // Ignore SIGPIPE (broken pipe when peer dies)
    signal(SIGPIPE, SIG_IGN);

    kvs::NodeConfig cfg;
    bool show_help = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            show_help = true;
        } else if (arg == "--id" && i + 1 < argc) {
            cfg.id = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            cfg.port = std::stoi(argv[++i]);
        } else if (arg == "--peer" && i + 1 < argc) {
            cfg.peer_addr = argv[++i];
        } else if (arg == "--log_path" && i + 1 < argc) {
            cfg.log_path = argv[++i];
        } else if (arg == "--run_id" && i + 1 < argc) {
            cfg.run_id = argv[++i];
        } else if (arg == "--hb_interval_ms" && i + 1 < argc) {
            cfg.hb_interval_ms = std::stoi(argv[++i]);
        } else if (arg == "--hb_timeout_ms" && i + 1 < argc) {
            cfg.hb_timeout_ms = std::stoi(argv[++i]);
        } else if (arg == "--repl_mode" && i + 1 < argc) {
            cfg.repl_mode = argv[++i];
        } else if (arg == "--primary") {
            cfg.is_primary = true;
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            usage(argv[0]);
            return 1;
        }
    }

    if (show_help) {
        usage(argv[0]);
        return 0;
    }

    // Read run_id from environment if not set via CLI
    if (cfg.run_id == "default_run") {
        const char* env_run_id = std::getenv("RUN_ID");
        if (env_run_id && env_run_id[0] != '\0') {
            cfg.run_id = env_run_id;
        }
    }

    // Validate required arguments
    if (cfg.id.empty() || cfg.port <= 0 || cfg.log_path.empty()) {
        std::cerr << "Error: --id, --port, and --log_path are required\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.is_primary && cfg.peer_addr.empty()) {
        std::cerr << "Error: --primary requires --peer <host:port>\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.repl_mode != "none" && cfg.repl_mode != "sync" && cfg.repl_mode != "async") {
        std::cerr << "Error: --repl_mode must be none, sync, or async\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.hb_interval_ms <= 0 || cfg.hb_timeout_ms <= 0) {
        std::cerr << "Error: --hb_interval_ms and --hb_timeout_ms must be positive\n\n";
        usage(argv[0]);
        return 1;
    }

    // Set up signal handlers for clean shutdown
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Create and run the node
    kvs::Node node(cfg);
    g_node = &node;
    node.run();
    g_node = nullptr;

    return 0;
}
