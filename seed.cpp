/*
 * seed.cpp
 * Seed node for the gossip-based P2P network.
 *
 * Responsibilities:
 *  1. Accept peer registration requests and run consensus with other seeds
 *     before admitting a peer to the Peer List (PL).
 *  2. Serve peer lists to joining peers.
 *  3. Receive dead-node reports from peers, run seed-level consensus,
 *     and remove confirmed dead peers from the PL.
 *  4. Forward dead-node confirmations to all live peers.
 *
 * Build:  g++ -std=c++17 -pthread -o seed seed.cpp
 * Usage:  ./seed <port> [config.txt]
 *
 * The config file lists all seed IP:Port pairs (including self).
 */

#include "common.h"

// ─────────────────────────────────────────────────────────────────────────────
//  Global state
// ─────────────────────────────────────────────────────────────────────────────

static std::string  g_selfIP;
static int          g_selfPort = 0;
static Logger*      g_log      = nullptr;

// Peer List: seeds maintain this set of known, registered peers
static std::set<NodeInfo>  g_peerList;
static std::mutex          g_plMu;

// Other seed nodes (excludes self)
static std::vector<NodeInfo> g_seeds;
static int g_numSeeds = 0;   // total seeds (including self)

// ─── Consensus state for REGISTRATION ─────────────────────────────────────
struct RegProposal {
    NodeInfo peer;
    int      yesVotes  = 1;  // proposer counts as YES
    int      noVotes   = 0;
    bool     committed = false;
    std::set<std::string> voters;  // track which seeds have voted
};
static std::map<std::string, RegProposal> g_regProposals;
static std::mutex                          g_regMu;

// ─── Consensus state for DEAD-NODE REMOVAL ────────────────────────────────
struct DeadProposal {
    NodeInfo dead;
    int      yesVotes  = 1;
    int      noVotes   = 0;
    bool     committed = false;
    std::set<std::string> voters;
};
static std::map<std::string, DeadProposal> g_deadProposals;
static std::mutex                           g_deadMu;

// Connected peer sockets (for forwarding REMOVE_PEER)
static std::map<std::string, int> g_peerSocks;  // key = "IP:Port"
static std::mutex                 g_peerSockMu;

// ─────────────────────────────────────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────────────────────────────────────

int quorum() {
    return g_numSeeds / 2 + 1;
}

std::string peerListStr() {
    std::lock_guard<std::mutex> lk(g_plMu);
    std::string s;
    for (auto& p : g_peerList) {
        if (!s.empty()) s += ",";
        s += p.str();
    }
    return s;
}

// Broadcast a message to all other seed nodes (best-effort, non-blocking)
void broadcastToSeeds(const std::string& msg) {
    for (auto& seed : g_seeds) {
        int s = connectTo(seed.ip, seed.port, 2);
        if (s < 0) continue;
        sendAll(s, msg + "\n");
        close(s);
    }
}

// Send REMOVE_PEER notification to all connected peers
void notifyPeersRemoval(const NodeInfo& dead) {
    std::string msg = std::string(MSG_REMOVE_PEER) + ":" + dead.str() + "\n";
    std::lock_guard<std::mutex> lk(g_peerSockMu);
    for (auto& [key, sock] : g_peerSocks) {
        sendAll(sock, msg);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  Registration consensus
// ─────────────────────────────────────────────────────────────────────────────

// Called when WE decide to propose a peer (because a peer connected to us)
void proposeRegistration(const NodeInfo& peer) {
    std::string key = peer.str();

    {
        std::lock_guard<std::mutex> lk(g_regMu);
        if (g_regProposals.count(key) && g_regProposals[key].committed) return;
        RegProposal& rp = g_regProposals[key];
        rp.peer = peer;
        rp.yesVotes = 1;
        rp.voters.insert(g_selfIP + ":" + std::to_string(g_selfPort));
    }

    g_log->log("REG_PROPOSAL: Proposing registration of peer " + key);

    // Send proposal to all other seeds
    std::string msg = std::string(MSG_REGISTER_PROPOSAL) + ":" + key
                    + ":" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n";
    broadcastToSeeds(msg);

    // If we are the only seed, commit immediately
    if (g_numSeeds == 1) {
        std::lock_guard<std::mutex> lk(g_regMu);
        auto& rp = g_regProposals[key];
        if (!rp.committed) {
            rp.committed = true;
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                g_peerList.insert(peer);
            }
            g_log->log("REG_COMMIT (single-seed): Peer " + key + " registered.");
        }
    }
}

// Called when another seed votes YES on our proposal (or when we receive a
// proposal from another seed and cast our own vote)
void handleRegVote(const std::string& peerKey, const std::string& voterKey, bool yes) {
    std::lock_guard<std::mutex> lk(g_regMu);
    auto it = g_regProposals.find(peerKey);
    if (it == g_regProposals.end()) return;
    RegProposal& rp = it->second;
    if (rp.committed) return;
    if (rp.voters.count(voterKey)) return;  // duplicate vote
    rp.voters.insert(voterKey);
    if (yes) rp.yesVotes++; else rp.noVotes++;

    g_log->log("REG_VOTE: peer=" + peerKey + " yes=" + std::to_string(rp.yesVotes)
             + " no=" + std::to_string(rp.noVotes));

    if (rp.yesVotes >= quorum() && !rp.committed) {
        rp.committed = true;
        NodeInfo p = rp.peer;
        {
            std::lock_guard<std::mutex> pl(g_plMu);
            g_peerList.insert(p);
        }
        g_log->log("REG_COMMIT: Peer " + peerKey + " registered after consensus.");
        // Broadcast commit to other seeds so they also add the peer
        std::string cm = std::string(MSG_REGISTER_COMMIT) + ":" + peerKey + "\n";
        broadcastToSeeds(cm);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  Dead-node consensus
// ─────────────────────────────────────────────────────────────────────────────

void proposeDeadNode(const NodeInfo& dead) {
    std::string key = dead.str();

    {
        std::lock_guard<std::mutex> lk(g_deadMu);
        if (g_deadProposals.count(key) && g_deadProposals[key].committed) return;
        DeadProposal& dp = g_deadProposals[key];
        dp.dead = dead;
        dp.yesVotes = 1;
        dp.voters.insert(g_selfIP + ":" + std::to_string(g_selfPort));
    }

    g_log->log("DEAD_PROPOSAL: Proposing removal of dead peer " + key);

    std::string msg = std::string(MSG_DEAD_PROPOSAL) + ":" + key
                    + ":" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n";
    broadcastToSeeds(msg);

    if (g_numSeeds == 1) {
        std::lock_guard<std::mutex> lk(g_deadMu);
        auto& dp = g_deadProposals[key];
        if (!dp.committed) {
            dp.committed = true;
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                g_peerList.erase(dead);
            }
            notifyPeersRemoval(dead);
            g_log->log("DEAD_COMMIT (single-seed): Peer " + key + " removed.");
        }
    }
}

void handleDeadVote(const std::string& deadKey, const std::string& voterKey, bool yes) {
    std::lock_guard<std::mutex> lk(g_deadMu);
    auto it = g_deadProposals.find(deadKey);
    if (it == g_deadProposals.end()) return;
    DeadProposal& dp = it->second;
    if (dp.committed) return;
    if (dp.voters.count(voterKey)) return;
    dp.voters.insert(voterKey);
    if (yes) dp.yesVotes++; else dp.noVotes++;

    g_log->log("DEAD_VOTE: dead=" + deadKey + " yes=" + std::to_string(dp.yesVotes));

    if (dp.yesVotes >= quorum() && !dp.committed) {
        dp.committed = true;
        NodeInfo d = dp.dead;
        {
            std::lock_guard<std::mutex> pl(g_plMu);
            g_peerList.erase(d);
        }
        notifyPeersRemoval(d);
        g_log->log("DEAD_COMMIT: Peer " + deadKey + " removed from PL after consensus.");
        std::string cm = std::string(MSG_DEAD_COMMIT) + ":" + deadKey + "\n";
        broadcastToSeeds(cm);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
//  Per-connection handler
// ─────────────────────────────────────────────────────────────────────────────

void handleConnection(int clientSock, std::string clientIP) {
    while (true) {
        std::string line = recvLine(clientSock);
        if (line.empty()) break;

        // ── REGISTER_PROPOSAL:peerIP:peerPort:proposerIP:proposerPort ──────
        if (line.rfind(MSG_REGISTER_PROPOSAL, 0) == 0) {
            // FORMAT: REGISTER_PROPOSAL:IP:Port:proposerIP:proposerPort
            auto parts = split(line, ':');
            if (parts.size() < 5) continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            std::string propKey = parts[3] + ":" + parts[4];

            NodeInfo peer = parseNode(peerKey);
            {
                std::lock_guard<std::mutex> lk(g_regMu);
                if (!g_regProposals.count(peerKey)) {
                    RegProposal& rp = g_regProposals[peerKey];
                    rp.peer = peer;
                    rp.yesVotes = 0;
                    rp.voters.insert(propKey);  // proposer is voter
                }
            }

            g_log->log("RECV REGISTER_PROPOSAL for " + peerKey + " from " + propKey);

            // Vote YES (we agree to add valid peers by default)
            // In production you'd do further validation here
            bool voteYes = true;
            std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
            std::string vote  = std::string(MSG_REGISTER_VOTE) + ":" + peerKey
                              + ":" + myKey + ":" + (voteYes ? "YES" : "NO") + "\n";

            // Send vote back to proposer
            int s = connectTo(parts[3], std::stoi(parts[4]), 2);
            if (s >= 0) { sendAll(s, vote); close(s); }

            // Also update our own tally
            handleRegVote(peerKey, myKey, voteYes);
        }

        // ── REGISTER_VOTE:peerKey:voterKey:YES/NO ─────────────────────────
        else if (line.rfind(MSG_REGISTER_VOTE, 0) == 0) {
            auto parts = split(line, ':');
            // peerKey = parts[1]:parts[2], voterKey = parts[3]:parts[4], vote = parts[5]
            if (parts.size() < 6) continue;
            std::string peerKey  = parts[1] + ":" + parts[2];
            std::string voterKey = parts[3] + ":" + parts[4];
            bool yes = (parts[5] == "YES");
            handleRegVote(peerKey, voterKey, yes);
        }

        // ── REGISTER_COMMIT:peerKey ────────────────────────────────────────
        else if (line.rfind(MSG_REGISTER_COMMIT, 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 3) continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            NodeInfo peer = parseNode(peerKey);
            {
                std::lock_guard<std::mutex> lk(g_regMu);
                g_regProposals[peerKey].committed = true;
            }
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                g_peerList.insert(peer);
            }
            g_log->log("REGISTER_COMMIT received: Peer " + peerKey + " added to PL.");
        }

        // ── REGISTER REQUEST from peer (direct peer→seed) ─────────────────
        // FORMAT:  REGISTER:IP:Port
        else if (line.rfind("REGISTER:", 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 3) continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            NodeInfo peer = parseNode(peerKey);

            // Check if already registered
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                if (g_peerList.count(peer)) {
                    sendAll(clientSock, std::string(MSG_REGISTER_OK) + ":" + peerKey + "\n");
                    continue;
                }
            }

            g_log->log("REGISTER request from peer " + peerKey);

            // Store the peer's socket for later use (serving peer list etc.)
            {
                std::lock_guard<std::mutex> lk(g_peerSockMu);
                g_peerSocks[peerKey] = clientSock;
            }

            // Start consensus
            proposeRegistration(peer);

            // Wait for consensus (poll for up to 10 seconds)
            bool ok = false;
            for (int i = 0; i < 100; i++) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                std::lock_guard<std::mutex> lk(g_regMu);
                auto it = g_regProposals.find(peerKey);
                if (it != g_regProposals.end() && it->second.committed) {
                    ok = true;
                    break;
                }
            }

            if (ok) {
                sendAll(clientSock, std::string(MSG_REGISTER_OK) + ":" + peerKey + "\n");
                g_log->log("Sent REGISTER_OK to peer " + peerKey);
            } else {
                sendAll(clientSock, std::string(MSG_REGISTER_FAIL) + ":" + peerKey + "\n");
                g_log->log("Sent REGISTER_FAIL to peer " + peerKey);
            }
        }

        // ── PEER_LIST request from peer ────────────────────────────────────
        else if (line == "GET_PEER_LIST") {
            std::string pl = MSG_PEER_LIST + std::string(":") + peerListStr() + "\n";
            sendAll(clientSock, pl);
            g_log->log("Sent PEER_LIST to client on socket " + std::to_string(clientSock));
        }

        // ── Dead Node report from peer ─────────────────────────────────────
        // FORMAT:  Dead Node:DeadIP:DeadPort:timestamp:reporterIP
        else if (line.rfind(MSG_DEAD_REPORT, 0) == 0) {
            // "Dead Node:IP:Port:ts:reporterIP"
            // split by ':' gives ["Dead Node", IP, Port, ts, reporterIP]
            // but "Dead Node" has a space so we handle it carefully
            size_t p1 = line.find(':');
            if (p1 == std::string::npos) continue;
            std::string rest = line.substr(p1 + 1);
            auto parts = split(rest, ':');
            if (parts.size() < 2) continue;
            std::string deadKey = parts[0] + ":" + parts[1];
            NodeInfo dead = parseNode(deadKey);

            g_log->log("Received Dead Node report for " + deadKey + " from " + clientIP);

            // Initiate removal consensus
            proposeDeadNode(dead);
        }

        // ── DEAD_PROPOSAL from another seed ───────────────────────────────
        else if (line.rfind(MSG_DEAD_PROPOSAL, 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 5) continue;
            std::string deadKey = parts[1] + ":" + parts[2];
            std::string propKey = parts[3] + ":" + parts[4];
            NodeInfo dead = parseNode(deadKey);

            {
                std::lock_guard<std::mutex> lk(g_deadMu);
                if (!g_deadProposals.count(deadKey)) {
                    DeadProposal& dp = g_deadProposals[deadKey];
                    dp.dead = dead;
                    dp.yesVotes = 0;
                    dp.voters.insert(propKey);
                }
            }
            // Vote YES
            std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
            std::string vote = std::string(MSG_DEAD_VOTE) + ":" + deadKey
                             + ":" + myKey + ":YES\n";
            int s = connectTo(parts[3], std::stoi(parts[4]), 2);
            if (s >= 0) { sendAll(s, vote); close(s); }
            handleDeadVote(deadKey, myKey, true);
        }

        // ── DEAD_VOTE ──────────────────────────────────────────────────────
        else if (line.rfind(MSG_DEAD_VOTE, 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 6) continue;
            std::string deadKey  = parts[1] + ":" + parts[2];
            std::string voterKey = parts[3] + ":" + parts[4];
            bool yes = (parts[5] == "YES");
            handleDeadVote(deadKey, voterKey, yes);
        }

        // ── DEAD_COMMIT from another seed ─────────────────────────────────
        else if (line.rfind(MSG_DEAD_COMMIT, 0) == 0) {
            auto parts = split(line, ':');
            if (parts.size() < 3) continue;
            std::string deadKey = parts[1] + ":" + parts[2];
            NodeInfo dead = parseNode(deadKey);
            {
                std::lock_guard<std::mutex> lk(g_deadMu);
                g_deadProposals[deadKey].committed = true;
            }
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                g_peerList.erase(dead);
            }
            notifyPeersRemoval(dead);
            g_log->log("DEAD_COMMIT received: Peer " + deadKey + " removed from PL.");
        }

        else {
            // Unknown message – ignore
        }
    }

    // Cleanup socket mapping
    {
        std::lock_guard<std::mutex> lk(g_peerSockMu);
        for (auto it = g_peerSocks.begin(); it != g_peerSocks.end(); ) {
            if (it->second == clientSock) it = g_peerSocks.erase(it);
            else ++it;
        }
    }
    close(clientSock);
}

// ─────────────────────────────────────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: ./seed <port> [config.txt]\n";
        return 1;
    }

    g_selfPort = std::stoi(argv[1]);

    // Determine own IP (use 127.0.0.1 for local testing; override via env)
    char* envIP = getenv("SELF_IP");
    g_selfIP = envIP ? envIP : "127.0.0.1";

    std::string configPath = (argc >= 3) ? argv[2] : "config.txt";
    std::vector<NodeInfo> allSeeds = readConfig(configPath);
    g_numSeeds = (int)allSeeds.size();

    if (g_numSeeds == 0) {
        std::cerr << "[ERROR] No seeds found in config.\n";
        return 1;
    }

    // Separate self from other seeds
    NodeInfo self{g_selfIP, g_selfPort};
    for (auto& s : allSeeds) {
        if (!(s == self)) g_seeds.push_back(s);
    }

    // Init logger
    std::string logFile = "seed_" + std::to_string(g_selfPort) + "_output.txt";
    g_log = new Logger(logFile);
    g_log->log("Seed node started at " + self.str() + " | total seeds=" + std::to_string(g_numSeeds)
             + " | quorum=" + std::to_string(quorum()));

    // Create listening socket
    int listenSock = createListenSocket(g_selfPort);
    if (listenSock < 0) return 1;

    g_log->log("Listening on port " + std::to_string(g_selfPort));

    // Accept loop
    while (true) {
        sockaddr_in caddr{};
        socklen_t   clen = sizeof(caddr);
        int csock = accept(listenSock, (sockaddr*)&caddr, &clen);
        if (csock < 0) { perror("accept"); continue; }

        char ipbuf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &caddr.sin_addr, ipbuf, sizeof(ipbuf));
        std::string cip(ipbuf);

        std::thread(handleConnection, csock, cip).detach();
    }

    close(listenSock);
    delete g_log;
    return 0;
}
