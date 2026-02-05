#pragma once
#include <string>
#include <unordered_map>

class Server {
public:
    explicit Server(int port);
    ~Server();

    bool start(std::string& err);
    void run(); // blocking

private:
    int port_;
    int listen_fd_ = -1;

    // demo用：インメモリKVS
    std::unordered_map<std::string, std::string> kv_;

    static bool send_all(int fd, const std::string& s);
    static bool read_line(int fd, std::string& buf, std::string& line); // "\r\n" まで

    void handle_client(int client_fd);
};