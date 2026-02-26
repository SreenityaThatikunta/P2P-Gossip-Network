#pragma once
/*
 * common.h
 * Shared constants, message types, utility functions, and data structures
 * used by both seed and peer nodes.
 */

#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <fstream>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <algorithm>
#include <map>
#include <set>
#include <mutex>
#include <thread>
#include <chrono>
#include <random>
#include <functional>
#include <atomic>
#include <condition_variable>

// ─────────────────────────────────────────────
//  Protocol message tags
// ─────────────────────────────────────────────
// Seed-level messages
#define MSG_REGISTER_PROPOSAL  "REGISTER_PROPOSAL"   // seed→seed: "can we add this peer?"
#define MSG_REGISTER_VOTE      "REGISTER_VOTE"       // seed→seed: YES/NO vote
#define MSG_REGISTER_COMMIT    "REGISTER_COMMIT"     // seed→seed: commit the addition
#define MSG_PEER_LIST          "PEER_LIST"            // seed→peer: list of peers
#define MSG_REGISTER_OK        "REGISTER_OK"         // seed→peer: registration confirmed
#define MSG_REGISTER_FAIL      "REGISTER_FAIL"       // seed→peer: registration failed
#define MSG_DEAD_REPORT        "Dead Node"            // peer→seed: suspected dead node
#define MSG_DEAD_PROPOSAL      "DEAD_PROPOSAL"       // seed→seed: propose removal
#define MSG_DEAD_VOTE          "DEAD_VOTE"            // seed→seed: vote on removal
#define MSG_DEAD_COMMIT        "DEAD_COMMIT"          // seed→seed: commit removal
#define MSG_REMOVE_PEER        "REMOVE_PEER"          // seed→peer: node removed

// Peer-level messages
#define MSG_GOSSIP             "GOSSIP"               // peer→peer: gossip payload
#define MSG_PING               "PING"                 // peer→peer: liveness probe
#define MSG_PONG               "PONG"                 // peer→peer: liveness reply
#define MSG_SUSPICION          "SUSPICION"            // peer→peer: I suspect X is dead
#define MSG_SUSPICION_CONFIRM  "SUSPICION_CONFIRM"   // peer→peer: I confirm X is dead

// ─────────────────────────────────────────────
//  Timing constants (seconds unless noted)
// ─────────────────────────────────────────────
#define GOSSIP_INTERVAL_SEC    5
#define MAX_GOSSIP_MSGS        10
#define PING_INTERVAL_SEC      3
#define PING_TIMEOUT_SEC       5
#define MISSED_PINGS_THRESHOLD 3    // how many consecutive missed pings before suspicion
#define SEED_CONNECT_TIMEOUT   5

// ─────────────────────────────────────────────
//  Buffer size
// ─────────────────────────────────────────────
#define BUF_SIZE 4096

// ─────────────────────────────────────────────
//  NodeInfo – identifies a node
// ─────────────────────────────────────────────
struct NodeInfo {
    std::string ip;
    int port;

    bool operator==(const NodeInfo& o) const {
        return ip == o.ip && port == o.port;
    }
    bool operator<(const NodeInfo& o) const {
        if (ip != o.ip) return ip < o.ip;
        return port < o.port;
    }
    std::string str() const {
        return ip + ":" + std::to_string(port);
    }
};

// ─────────────────────────────────────────────
//  Utility helpers
// ─────────────────────────────────────────────

// Get current wall-clock timestamp string
inline std::string currentTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    return std::string(buf);
}

// Get epoch seconds
inline long epochSec() {
    return (long)std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// Split a string by delimiter
inline std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, delim)) {
        if (!tok.empty()) parts.push_back(tok);
    }
    return parts;
}

// Trim whitespace
inline std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    size_t e = s.find_last_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    return s.substr(b, e - b + 1);
}

// Parse "IP:Port" into NodeInfo
inline NodeInfo parseNode(const std::string& s) {
    auto parts = split(s, ':');
    NodeInfo n;
    if (parts.size() >= 2) {
        n.ip = parts[0];
        n.port = std::stoi(parts[1]);
    }
    return n;
}

// Send a complete string over a socket (handles partial writes)
inline bool sendAll(int sock, const std::string& msg) {
    const char* ptr = msg.c_str();
    size_t total = msg.size();
    size_t sent = 0;
    while (sent < total) {
        ssize_t n = send(sock, ptr + sent, total - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += n;
    }
    return true;
}

// Receive a newline-terminated message from a socket
inline std::string recvLine(int sock) {
    std::string result;
    char c;
    while (true) {
        ssize_t n = recv(sock, &c, 1, 0);
        if (n <= 0) break;
        if (c == '\n') break;
        result += c;
    }
    return trim(result);
}

// Read seeds from config file: each line is "IP:Port"
inline std::vector<NodeInfo> readConfig(const std::string& path) {
    std::vector<NodeInfo> seeds;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "[ERROR] Cannot open config: " << path << "\n";
        return seeds;
    }
    std::string line;
    while (std::getline(f, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;
        // support comma-separated or newline-separated
        auto parts = split(line, ',');
        for (auto& p : parts) {
            p = trim(p);
            if (!p.empty()) seeds.push_back(parseNode(p));
        }
    }
    return seeds;
}

// Create and bind a TCP listening socket
inline int createListenSocket(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(sock); return -1;
    }
    if (listen(sock, 32) < 0) {
        perror("listen"); close(sock); return -1;
    }
    return sock;
}

// Connect to a remote host (with timeout)
inline int connectTo(const std::string& ip, int port, int timeoutSec = 5) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    // Set non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    connect(sock, (sockaddr*)&addr, sizeof(addr));

    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(sock, &wset);
    timeval tv{timeoutSec, 0};

    int r = select(sock + 1, nullptr, &wset, nullptr, &tv);
    if (r <= 0) { close(sock); return -1; }

    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err) { close(sock); return -1; }

    // Restore blocking
    fcntl(sock, F_SETFL, flags);
    return sock;
}

// Simple hash for message deduplication
inline std::string hashMsg(const std::string& msg) {
    std::hash<std::string> h;
    return std::to_string(h(msg));
}

// Logger – thread-safe writes to console and file
class Logger {
public:
    Logger(const std::string& filename) : filename_(filename) {
        file_.open(filename, std::ios::app);
        if (!file_.is_open())
            std::cerr << "[WARN] Cannot open log file: " << filename << "\n";
    }
    ~Logger() { if (file_.is_open()) file_.close(); }

    void log(const std::string& msg) {
        std::lock_guard<std::mutex> lk(mu_);
        std::string line = "[" + currentTimestamp() + "] " + msg;
        std::cout << line << "\n";
        if (file_.is_open()) {
            file_ << line << "\n";
            file_.flush();
        }
    }

private:
    std::string filename_;
    std::ofstream file_;
    std::mutex mu_;
};
