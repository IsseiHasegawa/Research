/**
 * net.hpp — TCP socket utilities for the distributed KV store.
 *
 * Provides thin wrappers around POSIX sockets:
 *   - tcp_listen()    : create a listening socket on a port
 *   - tcp_connect()   : connect to a remote host:port
 *   - send_msg()      : send a complete newline-delimited JSON message
 *   - recv_line()     : read one newline-terminated line from a socket
 *   - set_nonblocking(): toggle non-blocking mode
 *   - wait_readable()  : select()-based readability check with timeout
 *
 * Assumes POSIX (Linux / macOS). No Windows support in this prototype.
 */

#pragma once

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

// On macOS, MSG_NOSIGNAL is not defined; we rely on SIGPIPE being ignored
// via signal(SIGPIPE, SIG_IGN) in main(). Define MSG_NOSIGNAL as 0.
#ifdef __APPLE__
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#endif

namespace net {

// ─── Constants ────────────────────────────────────────────────────────────────

constexpr int INVALID_FD = -1;

// ─── Socket lifecycle ─────────────────────────────────────────────────────────

// Create a TCP socket, bind to port, and listen.
// Returns the listening fd, or INVALID_FD on failure.
inline int tcp_listen(int port, int backlog = 16) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return INVALID_FD;

    // Allow rapid rebind after process restart
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return INVALID_FD;
    }
    if (listen(fd, backlog) != 0) {
        close(fd);
        return INVALID_FD;
    }
    return fd;
}

// Connect to host:port. Returns the connected fd, or INVALID_FD on failure.
// Retries a few times with short delays to handle race conditions at startup.
inline int tcp_connect(const std::string& host, int port, int max_retries = 5,
                       int retry_delay_ms = 200) {
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        return INVALID_FD;
    }

    for (int attempt = 0; attempt < max_retries; ++attempt) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return INVALID_FD;

        if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            // Disable Nagle for low-latency messaging
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            return fd;
        }
        close(fd);

        if (attempt + 1 < max_retries) {
            usleep(retry_delay_ms * 1000);
        }
    }
    return INVALID_FD;
}

// Close a socket fd safely.
inline void close_fd(int fd) {
    if (fd >= 0) close(fd);
}

// ─── Non-blocking / select ────────────────────────────────────────────────────

inline bool set_nonblocking(int fd, bool enable) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    if (enable) flags |= O_NONBLOCK;
    else flags &= ~O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags) == 0;
}

// Wait for fd to become readable. Returns true if data is available.
// timeout_ms=0 means poll once without waiting.
inline bool wait_readable(int fd, int timeout_ms) {
    fd_set rset;
    FD_ZERO(&rset);
    FD_SET(fd, &rset);
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    int n = select(fd + 1, &rset, nullptr, nullptr, &tv);
    return n > 0 && FD_ISSET(fd, &rset);
}

// ─── Message I/O ──────────────────────────────────────────────────────────────

// Send all bytes. Returns true on success.
inline bool send_all(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = send(fd, data, len, MSG_NOSIGNAL);
        if (n <= 0) return false;
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Send a newline-terminated JSON string.
inline bool send_msg(int fd, const std::string& json_line) {
    return send_all(fd, json_line.data(), json_line.size());
}

/**
 * Read one newline-terminated line from the socket into *out.
 *
 * Uses a persistent buffer (passed by reference) to handle partial reads.
 * Returns true when a complete line has been read.
 * Returns false on EOF or error.
 *
 * If allow_timeout_retry is true, the socket should be non-blocking and we
 * use wait_readable() to poll — returning false from wait_readable does NOT
 * cause this function to return false; it keeps retrying. This is used by the
 * heartbeat receiver so that a select() timeout does not kill the read loop.
 */
inline bool recv_line(int fd, std::string* out, std::vector<char>& buf,
                      bool allow_timeout_retry = false,
                      int wait_ms = 100) {
    out->clear();
    for (;;) {
        // Check if we already have a full line in the buffer
        auto it = std::find(buf.begin(), buf.end(), '\n');
        if (it != buf.end()) {
            out->assign(buf.begin(), it);
            buf.erase(buf.begin(), it + 1);
            return true;
        }
        // Sanity: prevent unbounded buffer growth
        if (buf.size() > 1024 * 1024) return false;

        // Wait for data if in non-blocking + retry mode
        if (allow_timeout_retry && !wait_readable(fd, wait_ms)) {
            continue;  // timeout: no data yet, keep trying
        }

        char tmp[512];
        ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            buf.insert(buf.end(), tmp, tmp + n);
            continue;
        }
        if (n == 0) return false;  // EOF (peer closed)

        // n < 0: error
        if (allow_timeout_retry && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            continue;
        }
        return false;
    }
}

// ─── Address parsing ──────────────────────────────────────────────────────────

// Parse "host:port" into separate host string and port int.
// If no colon, assumes the whole string is a port with host "127.0.0.1".
inline bool parse_addr(const std::string& addr, std::string& host, int& port) {
    auto colon = addr.rfind(':');
    if (colon != std::string::npos) {
        host = addr.substr(0, colon);
        try { port = std::stoi(addr.substr(colon + 1)); }
        catch (...) { return false; }
    } else {
        host = "127.0.0.1";
        try { port = std::stoi(addr); }
        catch (...) { return false; }
    }
    return port > 0 && port <= 65535;
}

} // namespace net
