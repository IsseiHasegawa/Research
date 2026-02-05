#include "Server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>

Server::Server(int port) : port_(port) {}

Server::~Server() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

bool Server::start(std::string& err) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        err = std::string("socket: ") + ::strerror(errno);
        return false;
    }

    int yes = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); // 127.0.0.1
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        err = std::string("bind: ") + ::strerror(errno);
        return false;
    }
    if (::listen(listen_fd_, 16) < 0) {
        err = std::string("listen: ") + ::strerror(errno);
        return false;
    }
    return true;
}

bool Server::send_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool Server::read_line(int fd, std::string& buf, std::string& line) {
    while (true) {
        auto pos = buf.find("\r\n");
        if (pos != std::string::npos) {
            line = buf.substr(0, pos);
            buf.erase(0, pos + 2);
            return true;
        }
        char tmp[4096];
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) return false; // closed
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        buf.append(tmp, tmp + n);
    }
}

void Server::handle_client(int client_fd) {
    std::string buf, line;

    while (read_line(client_fd, buf, line)) {
        if (line.empty()) continue;

        // cmd = first token
        auto sp1 = line.find(' ');
        std::string cmd = (sp1 == std::string::npos) ? line : line.substr(0, sp1);

        if (cmd == "quit") {
            break;
        }

        if (cmd == "get") {
            std::string key = (sp1 == std::string::npos) ? "" : line.substr(sp1 + 1);
            auto it = kv_.find(key);
            if (it == kv_.end()) {
                send_all(client_fd, "NOT_FOUND\r\n");
            } else {
                send_all(client_fd, "VALUE " + it->second + "\r\n");
            }
            continue;
        }

        if (cmd == "delete") {
            std::string key = (sp1 == std::string::npos) ? "" : line.substr(sp1 + 1);
            if (kv_.erase(key) > 0) send_all(client_fd, "DELETED\r\n");
            else send_all(client_fd, "NOT_FOUND\r\n");
            continue;
        }

        if (cmd == "set") {
            // set <key> <value...>
            if (sp1 == std::string::npos) {
                send_all(client_fd, "ERROR\r\n");
                continue;
            }
            std::string rest = line.substr(sp1 + 1);
            auto sp2 = rest.find(' ');
            if (sp2 == std::string::npos) {
                send_all(client_fd, "ERROR\r\n");
                continue;
            }
            std::string key = rest.substr(0, sp2);
            std::string value = rest.substr(sp2 + 1); // 残り全部をvalueに
            kv_[std::move(key)] = std::move(value);
            send_all(client_fd, "STORED\r\n");
            continue;
        }

        send_all(client_fd, "ERROR\r\n");
    }

    ::close(client_fd);
}

void Server::run() {
    std::cout << "demo-memcached listening on 127.0.0.1:" << port_ << "\n";

    while (true) {
        int cfd = ::accept(listen_fd_, nullptr, nullptr);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "accept: " << ::strerror(errno) << "\n";
            return;
        }
        handle_client(cfd);
    }
}