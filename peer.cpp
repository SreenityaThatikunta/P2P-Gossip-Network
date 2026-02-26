/*
 * peer.cpp
 * Peer node for the gossip-based P2P network.
 *
 * Responsibilities:
 *  1. Register with ⌊n/2⌋+1 seed nodes (consensus-based).
 *  2. Request peer lists from registered seeds, compute union,
 *     select neighbors following a power-law degree distribution.
 *  3. Gossip: broadcast messages every 5 seconds (max 10 messages).
 *  4. Forward gossip: on first receipt, forward to all neighbors except sender.
 *  5. Liveness: ping neighbors periodically; run two-level consensus
 *     before reporting dead nodes to seeds.
 */

#include "common.h"

static std::string g_selfIP;
static int         g_selfPort = 0;
static Logger*     g_log      = nullptr;

static std::vector<NodeInfo> g_registeredSeeds;
static std::mutex             g_seedMu;

static std::vector<NodeInfo> g_allSeeds;
static int g_numSeeds = 0;

static std::map<std::string, int> g_neighborSocks;
static std::mutex                  g_neighborMu;

static std::map<std::string, std::set<std::string>> g_topology;
static std::mutex                                     g_topoMu;

static std::map<std::string, std::string> g_messageList;
static std::mutex                          g_mlMu;

static std::atomic<int> g_msgCounter{0};

struct SuspicionEntry {
    NodeInfo node;
    std::set<std::string> confirmers;  
    bool reported = false;             
};
static std::map<std::string, SuspicionEntry> g_suspicions;
static std::mutex                             g_suspMu;

static std::map<std::string, int> g_missedPings;
static std::mutex                  g_pingMu;

//  Power-law neighbor selection
int sampleZipf(int maxK, double alpha = 1.5) {
    double norm = 0.0;
    for (int i = 1; i <= maxK; i++) norm += 1.0 / std::pow(i, alpha);

    std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double r = dist(rng);
    double cumul = 0.0;
    for (int i = 1; i <= maxK; i++) {
        cumul += (1.0 / std::pow(i, alpha)) / norm;
        if (r <= cumul) return i;
    }
    return maxK;
}

std::vector<NodeInfo> selectNeighbors(const std::vector<NodeInfo>& candidates) {
    if (candidates.empty()) return {};

    int pool = (int)candidates.size();
    int k    = std::max(1, sampleZipf(std::min(pool, 8)));

    std::vector<double> weights(pool, 1.0);
    {
        std::lock_guard<std::mutex> lk(g_topoMu);
        for (int i = 0; i < pool; i++) {
            auto it = g_topology.find(candidates[i].str());
            if (it != g_topology.end())
                weights[i] = 1.0 + (double)it->second.size();
        }
    }

    std::mt19937 rng(std::random_device{}());
    std::vector<int> indices(pool);
    std::iota(indices.begin(), indices.end(), 0);
    std::vector<NodeInfo> chosen;
    for (int i = 0; i < k && !indices.empty(); i++) {
        std::vector<double> w;
        for (int idx : indices) w.push_back(weights[idx]);
        std::discrete_distribution<int> ddist(w.begin(), w.end());
        int pick = ddist(rng);
        chosen.push_back(candidates[indices[pick]]);
        indices.erase(indices.begin() + pick);
    }
    return chosen;
}

//  Topology management
void addEdge(const std::string& a, const std::string& b) {
    std::lock_guard<std::mutex> lk(g_topoMu);
    g_topology[a].insert(b);
    g_topology[b].insert(a);
}

void removeNode(const std::string& key) {
    std::lock_guard<std::mutex> lk(g_topoMu);
    g_topology.erase(key);
    for (auto& [k, nbrs] : g_topology) nbrs.erase(key);
}

//  Sending helpers
void broadcastGossip(const std::string& msg, const std::string& excludeKey = "") {
    std::lock_guard<std::mutex> lk(g_neighborMu);
    for (auto& [key, sock] : g_neighborSocks) {
        if (key == excludeKey) continue;
        sendAll(sock, std::string(MSG_GOSSIP) + ":" + msg + "\n");
    }
}

void sendToSeeds(const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_seedMu);
    for (auto& seed : g_registeredSeeds) {
        int s = connectTo(seed.ip, seed.port, 3);
        if (s < 0) continue;
        sendAll(s, msg + "\n");
        close(s);
    }
}

void broadcastSuspicion(const std::string& suspKey) {
    std::lock_guard<std::mutex> lk(g_neighborMu);
    std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
    std::string msg = std::string(MSG_SUSPICION) + ":" + suspKey + ":" + myKey + "\n";
    for (auto& [key, sock] : g_neighborSocks) {
        sendAll(sock, msg);
    }
}

//  Report dead node to seeds (after peer-level consensus)
void reportDeadToSeeds(const NodeInfo& dead) {
    std::string ts    = std::to_string(epochSec());
    std::string myIP  = g_selfIP;
    std::string msg   = std::string(MSG_DEAD_REPORT) + ":"
                      + dead.ip + ":" + std::to_string(dead.port) + ":"
                      + ts + ":" + myIP;
    sendToSeeds(msg);
    g_log->log("DEAD REPORT sent to seeds for " + dead.str());
}

//  Suspicion / peer-level consensus
int neighborCount() {
    std::lock_guard<std::mutex> lk(g_neighborMu);
    return (int)g_neighborSocks.size();
}

void handleSuspicion(const std::string& suspKey, const std::string& reporterKey) {
    int nbrs = neighborCount();
    int peerQuorum = nbrs / 2 + 1;

    std::lock_guard<std::mutex> lk(g_suspMu);
    auto& entry = g_suspicions[suspKey];
    entry.node = parseNode(suspKey);
    entry.confirmers.insert(reporterKey);
    if (g_missedPings.count(suspKey)) {
        std::lock_guard<std::mutex> plk(g_pingMu);
        if (g_missedPings[suspKey] >= MISSED_PINGS_THRESHOLD)
            entry.confirmers.insert(g_selfIP + ":" + std::to_string(g_selfPort));
    }

    g_log->log("SUSPICION received about " + suspKey + " from " + reporterKey
             + " (" + std::to_string(entry.confirmers.size()) + "/" + std::to_string(peerQuorum) + ")");

    if ((int)entry.confirmers.size() >= peerQuorum && !entry.reported) {
        entry.reported = true;
        g_log->log("PEER-LEVEL CONSENSUS: " + suspKey + " confirmed dead. Reporting to seeds.");
        {
            std::lock_guard<std::mutex> nl(g_neighborMu);
            auto it = g_neighborSocks.find(suspKey);
            if (it != g_neighborSocks.end()) {
                close(it->second);
                g_neighborSocks.erase(it);
            }
        }
        removeNode(suspKey);
        reportDeadToSeeds(entry.node);
    }
}

//  Incoming connection handler (neighbors connect to us)
void handleIncomingNeighbor(int sock, std::string remoteKey) {
    {
        std::lock_guard<std::mutex> lk(g_neighborMu);
        g_neighborSocks[remoteKey] = sock;
    }
    addEdge(g_selfIP + ":" + std::to_string(g_selfPort), remoteKey);
    g_log->log("Neighbor connected: " + remoteKey);

    while (true) {
        std::string line = recvLine(sock);
        if (line.empty()) break;

        if (line.rfind(std::string(MSG_GOSSIP) + ":", 0) == 0) {
            std::string payload = line.substr(strlen(MSG_GOSSIP) + 1);
            std::string h = hashMsg(payload);
            bool isNew = false;
            {
                std::lock_guard<std::mutex> lk(g_mlMu);
                if (!g_messageList.count(h)) {
                    g_messageList[h] = payload;
                    isNew = true;
                }
            }
            if (isNew) {
                g_log->log("GOSSIP received (first time): " + payload + " from " + remoteKey);
                broadcastGossip(payload, remoteKey);
            }
        }

        else if (line == MSG_PING) {
            sendAll(sock, std::string(MSG_PONG) + "\n");
        }

        else if (line == MSG_PONG) {
            std::lock_guard<std::mutex> lk(g_pingMu);
            g_missedPings[remoteKey] = 0;
        }

        else if (line.rfind(std::string(MSG_SUSPICION) + ":", 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 5) continue;
            std::string suspKey    = parts[1] + ":" + parts[2];
            std::string reportKey  = parts[3] + ":" + parts[4];
            handleSuspicion(suspKey, reportKey);
        }

        else if (line.rfind(MSG_REMOVE_PEER, 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 3) continue;
            std::string deadKey = parts[1] + ":" + parts[2];
            g_log->log("Seed confirmed dead node removed: " + deadKey);
            {
                std::lock_guard<std::mutex> lk(g_neighborMu);
                auto it = g_neighborSocks.find(deadKey);
                if (it != g_neighborSocks.end()) {
                    close(it->second);
                    g_neighborSocks.erase(it);
                }
            }
            removeNode(deadKey);
        }
    }

    {
        std::lock_guard<std::mutex> lk(g_neighborMu);
        g_neighborSocks.erase(remoteKey);
    }
    removeNode(remoteKey);
    close(sock);
    g_log->log("Neighbor disconnected: " + remoteKey);
}

//  Ping loop (runs in a background thread)
void pingLoop() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(PING_INTERVAL_SEC));

        std::vector<std::pair<std::string,int>> snapshot;
        {
            std::lock_guard<std::mutex> lk(g_neighborMu);
            for (auto& [k, s] : g_neighborSocks) snapshot.emplace_back(k, s);
        }

        for (auto& [key, sock] : snapshot) {
            sendAll(sock, std::string(MSG_PING) + "\n");

            fd_set rset;
            FD_ZERO(&rset);
            FD_SET(sock, &rset);
            timeval tv{PING_TIMEOUT_SEC, 0};
            int r = select(sock + 1, &rset, nullptr, nullptr, &tv);
            bool gotPong = false;
            if (r > 0) {
                std::string resp = recvLine(sock);
                if (resp == MSG_PONG) gotPong = true;
            }

            bool suspect = false;
            {
                std::lock_guard<std::mutex> lk(g_pingMu);
                if (gotPong) {
                    g_missedPings[key] = 0;
                } else {
                    g_missedPings[key]++;
                    int missed = g_missedPings[key];
                    if (missed >= MISSED_PINGS_THRESHOLD) {
                        g_log->log("SUSPICION: " + key + " has missed "
                                 + std::to_string(missed) + " pings. Broadcasting suspicion.");
                        suspect = true;
                    }
                }
            }
            if (suspect) {
                broadcastSuspicion(key);
                handleSuspicion(key,
                    g_selfIP + ":" + std::to_string(g_selfPort));
            }
        }
    }
}

//  Gossip generation loop (runs in a background thread)
void gossipLoop() {
    std::this_thread::sleep_for(std::chrono::seconds(2));

    while (g_msgCounter < MAX_GOSSIP_MSGS) {
        int msgNum = ++g_msgCounter;
        std::string ts      = std::to_string(epochSec());
        std::string payload = ts + ":" + g_selfIP + ":" + std::to_string(msgNum);

        std::string h = hashMsg(payload);
        {
            std::lock_guard<std::mutex> lk(g_mlMu);
            g_messageList[h] = payload;
        }

        broadcastGossip(payload);
        g_log->log("GOSSIP sent [" + std::to_string(msgNum) + "]: " + payload);

        std::this_thread::sleep_for(std::chrono::seconds(GOSSIP_INTERVAL_SEC));
    }
    g_log->log("Gossip generation complete (sent " + std::to_string(MAX_GOSSIP_MSGS) + " messages).");
}

//  Accept loop (for incoming neighbor connections)
void acceptLoop(int listenSock) {
    while (true) {
        sockaddr_in caddr{};
        socklen_t   clen = sizeof(caddr);
        int csock = accept(listenSock, (sockaddr*)&caddr, &clen);
        if (csock < 0) { perror("accept"); continue; }

        char ipbuf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &caddr.sin_addr, ipbuf, sizeof(ipbuf));
        int remotePort = ntohs(caddr.sin_port);

        std::string remoteKey = std::string(ipbuf) + ":" + std::to_string(remotePort);

        std::string hello = recvLine(csock);
        if (hello.rfind("HELLO:", 0) == 0) {
            auto parts = split(hello, ':');
            if (parts.size() >= 3)
                remoteKey = parts[1] + ":" + parts[2];
        }

        std::thread(handleIncomingNeighbor, csock, remoteKey).detach();
    }
}

//  Connect to a neighbor (outgoing)
bool connectNeighbor(const NodeInfo& nb) {
    std::string key = nb.str();
    {
        std::lock_guard<std::mutex> lk(g_neighborMu);
        if (g_neighborSocks.count(key)) return true; 
    }

    int sock = connectTo(nb.ip, nb.port);
    if (sock < 0) {
        g_log->log("Cannot connect to neighbor " + key);
        return false;
    }

    sendAll(sock, "HELLO:" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n");

    {
        std::lock_guard<std::mutex> lk(g_neighborMu);
        g_neighborSocks[key] = sock;
    }
    addEdge(g_selfIP + ":" + std::to_string(g_selfPort), key);
    g_log->log("Connected to neighbor " + key);

    std::thread(handleIncomingNeighbor, sock, key).detach();
    return true;
}

//  Seed interaction
bool registerWithSeed(const NodeInfo& seed) {
    int sock = connectTo(seed.ip, seed.port, SEED_CONNECT_TIMEOUT);
    if (sock < 0) {
        g_log->log("Cannot reach seed " + seed.str());
        return false;
    }

    sendAll(sock, "REGISTER:" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n");

    std::string resp = recvLine(sock);
    bool ok = (resp.rfind(MSG_REGISTER_OK, 0) == 0);
    if (ok) g_log->log("Registered with seed " + seed.str());
    else    g_log->log("Registration failed/timeout with seed " + seed.str() + " resp=" + resp);

    close(sock);
    return ok;
}

std::vector<NodeInfo> getPeerListFromSeed(const NodeInfo& seed) {
    std::vector<NodeInfo> peers;
    int sock = connectTo(seed.ip, seed.port, SEED_CONNECT_TIMEOUT);
    if (sock < 0) return peers;

    sendAll(sock, "GET_PEER_LIST\n");
    std::string resp = recvLine(sock);
    close(sock);

    if (resp.rfind(std::string(MSG_PEER_LIST) + ":", 0) == 0) {
        std::string list = resp.substr(strlen(MSG_PEER_LIST) + 1);
        auto parts = split(list, ',');
        for (auto& p : parts) {
            NodeInfo n = parseNode(p);
            if (!n.ip.empty()) peers.push_back(n);
        }
        g_log->log("Peer list from seed " + seed.str() + ": " + list);
    }
    return peers;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: ./peer <port> [config.txt]\n";
        return 1;
    }

    g_selfPort = std::stoi(argv[1]);

    char* envIP = getenv("SELF_IP");
    g_selfIP = envIP ? envIP : "127.0.0.1";

    std::string configPath = (argc >= 3) ? argv[2] : "config.txt";
    g_allSeeds = readConfig(configPath);
    g_numSeeds = (int)g_allSeeds.size();

    if (g_numSeeds == 0) {
        std::cerr << "[ERROR] No seeds in config.\n";
        return 1;
    }

    std::string logFile = "peer_" + std::to_string(g_selfPort) + "_output.txt";
    g_log = new Logger(logFile);

    std::string selfKey = g_selfIP + ":" + std::to_string(g_selfPort);
    g_log->log("Peer started at " + selfKey + " | seeds=" + std::to_string(g_numSeeds));

    int listenSock = createListenSocket(g_selfPort);
    if (listenSock < 0) return 1;

    std::thread(acceptLoop, listenSock).detach();

    int required = g_numSeeds / 2 + 1;
    g_log->log("Need to register with " + std::to_string(required) + " of "
             + std::to_string(g_numSeeds) + " seeds.");

    std::vector<NodeInfo> shuffled = g_allSeeds;
    std::mt19937 rng(std::random_device{}());
    std::shuffle(shuffled.begin(), shuffled.end(), rng);

    int registered = 0;
    for (auto& seed : shuffled) {
        if (registered >= required) break;
        if (registerWithSeed(seed)) {
            std::lock_guard<std::mutex> lk(g_seedMu);
            g_registeredSeeds.push_back(seed);
            registered++;
        }
    }

    if (registered < required) {
        g_log->log("[ERROR] Could only register with " + std::to_string(registered)
                 + " seeds (need " + std::to_string(required) + "). Aborting.");
        close(listenSock);
        delete g_log;
        return 1;
    }
    g_log->log("Successfully registered with " + std::to_string(registered) + " seeds.");

    std::set<NodeInfo> allPeers;
    {
        std::lock_guard<std::mutex> lk(g_seedMu);
        for (auto& seed : g_registeredSeeds) {
            auto pl = getPeerListFromSeed(seed);
            for (auto& p : pl) allPeers.insert(p);
        }
    }

    NodeInfo self{g_selfIP, g_selfPort};
    allPeers.erase(self);

    std::vector<NodeInfo> candidates(allPeers.begin(), allPeers.end());
    g_log->log("Total candidate peers: " + std::to_string(candidates.size()));

    std::vector<NodeInfo> neighbors = selectNeighbors(candidates);
    g_log->log("Selected " + std::to_string(neighbors.size()) + " neighbors.");

    for (auto& nb : neighbors) {
        connectNeighbor(nb);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::thread gossipThread(gossipLoop);
    std::thread pingThread(pingLoop);

    gossipThread.join();  
    pingThread.detach();  

    g_log->log("Gossip phase complete. Continuing liveness monitoring.");

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }

    close(listenSock);
    delete g_log;
    return 0;
}
