/*
 * peer.cpp
 * Peer node for the gossip-based P2P network.
 *
 * Key design decisions:
 *  - Pings are sent on the EXISTING persistent gossip socket (not new connections).
 *    This avoids ephemeral-port connections being mistaken for neighbors.
 *  - PONG is received by the same handleIncomingNeighbor recv-loop thread.
 *    A per-neighbor atomic "pong received" flag communicates back to pingLoop
 *    without any select() race on the shared socket.
 *  - Deduplication key is "senderIP:msgNum", not hash(full payload+timestamp).
 *  - Minimum 2 neighbors enforced for connectivity.
 *
 * Build:  g++ -std=c++17 -pthread -o peer peer.cpp
 * Usage:  ./peer <port> [config.txt]
 */

 #include "common.h"

 // ─────────────────────────────────────────────────────────────────────────────
 //  Global state
 // ─────────────────────────────────────────────────────────────────────────────
 
 static std::string g_selfIP;
 static int         g_selfPort = 0;
 static Logger*     g_log      = nullptr;
 
 static std::vector<NodeInfo> g_registeredSeeds;
 static std::mutex             g_seedMu;
 
 static std::vector<NodeInfo> g_allSeeds;
 static int g_numSeeds = 0;
 
 // Neighbor sockets: key = "IP:listenPort", value = socket fd
 static std::map<std::string, int> g_neighborSocks;
 static std::mutex                  g_neighborMu;
 
 // Per-neighbor pong flags: set by recv thread, consumed by ping thread
 static std::map<std::string, std::atomic<bool>*> g_pongFlags;
 static std::mutex                                  g_pongMu;
 
 // Topology adjacency list
 static std::map<std::string, std::set<std::string>> g_topology;
 static std::mutex                                     g_topoMu;
 
 // Message dedup: "senderIP:msgNum" -> payload
 static std::map<std::string, std::string> g_messageList;
 static std::mutex                          g_mlMu;
 
 static std::atomic<int> g_msgCounter{0};
 
 // Suspicion tracking
 struct SuspicionEntry {
     NodeInfo node;
     std::set<std::string> confirmers;
     int  quorum   = 1;
     bool reported = false;
 };
 static std::map<std::string, SuspicionEntry> g_suspicions;
 static std::mutex                             g_suspMu;
 
 // Missed ping counts
 static std::map<std::string, int> g_missedPings;
 static std::mutex                  g_pingMu;
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Forward declarations
 // ─────────────────────────────────────────────────────────────────────────────
 void handleSuspicion(const std::string& suspKey, const std::string& reporterKey);
 void broadcastSuspicion(const std::string& suspKey);
 void reportDeadToSeeds(const NodeInfo& dead);
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Power-law neighbor selection (Zipf + preferential attachment)
 // ─────────────────────────────────────────────────────────────────────────────
 
 int sampleZipf(int maxK, double alpha = 1.5) {
     double norm = 0.0;
     for (int i = 1; i <= maxK; i++) norm += 1.0 / std::pow(i, alpha);
     std::mt19937 rng(std::random_device{}());
     std::uniform_real_distribution<double> dist(0.0, 1.0);
     double r = dist(rng), cumul = 0.0;
     for (int i = 1; i <= maxK; i++) {
         cumul += (1.0 / std::pow(i, alpha)) / norm;
         if (r <= cumul) return i;
     }
     return maxK;
 }
 
 std::vector<NodeInfo> selectNeighbors(const std::vector<NodeInfo>& candidates) {
     if (candidates.empty()) return {};
     int pool = (int)candidates.size();
     int k = std::min(pool, std::max(2, sampleZipf(std::min(pool, 8))));
 
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
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Topology helpers
 // ─────────────────────────────────────────────────────────────────────────────
 
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
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Pong flag helpers
 // ─────────────────────────────────────────────────────────────────────────────
 
 void registerPongFlag(const std::string& key) {
     std::lock_guard<std::mutex> lk(g_pongMu);
     if (!g_pongFlags.count(key))
         g_pongFlags[key] = new std::atomic<bool>(false);
 }
 
 void removePongFlag(const std::string& key) {
     std::lock_guard<std::mutex> lk(g_pongMu);
     auto it = g_pongFlags.find(key);
     if (it != g_pongFlags.end()) { delete it->second; g_pongFlags.erase(it); }
 }
 
 // Called by recv thread when PONG arrives
 void setPongFlag(const std::string& key) {
     std::lock_guard<std::mutex> lk(g_pongMu);
     auto it = g_pongFlags.find(key);
     if (it != g_pongFlags.end()) it->second->store(true);
 }
 
 // Called by ping thread — returns true if pong arrived, resets flag
 bool consumePong(const std::string& key) {
     std::lock_guard<std::mutex> lk(g_pongMu);
     auto it = g_pongFlags.find(key);
     if (it == g_pongFlags.end()) return false;
     return it->second->exchange(false);
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Broadcast helpers
 // ─────────────────────────────────────────────────────────────────────────────
 
 void broadcastGossip(const std::string& payload, const std::string& excludeKey = "") {
     std::lock_guard<std::mutex> lk(g_neighborMu);
     for (auto& [key, sock] : g_neighborSocks) {
         if (key == excludeKey) continue;
         sendAll(sock, std::string(MSG_GOSSIP) + ":" + payload + "\n");
     }
 }
 
 void broadcastSuspicion(const std::string& suspKey) {
     std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
     std::string msg = std::string(MSG_SUSPICION) + ":" + suspKey + ":" + myKey + "\n";
     std::lock_guard<std::mutex> lk(g_neighborMu);
     for (auto& [key, sock] : g_neighborSocks) sendAll(sock, msg);
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
 
 void reportDeadToSeeds(const NodeInfo& dead) {
     std::string msg = std::string(MSG_DEAD_REPORT) + ":"
                     + dead.ip + ":" + std::to_string(dead.port) + ":"
                     + std::to_string(epochSec()) + ":" + g_selfIP;
     sendToSeeds(msg);
     g_log->log("DEAD REPORT sent to seeds for " + dead.str());
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Suspicion / peer-level consensus
 // ─────────────────────────────────────────────────────────────────────────────
 
 void handleSuspicion(const std::string& suspKey, const std::string& reporterKey) {
     std::lock_guard<std::mutex> lk(g_suspMu);
     auto& entry = g_suspicions[suspKey];
     if (entry.reported) return;
 
     if (entry.node.ip.empty()) {
         entry.node = parseNode(suspKey);
         int nbrs;
         {
             std::lock_guard<std::mutex> nl(g_neighborMu);
             nbrs = (int)g_neighborSocks.size();
             if (!g_neighborSocks.count(suspKey)) nbrs += 1;
         }
         entry.quorum = std::max(1, nbrs / 2 + 1);
     }
 
     entry.confirmers.insert(reporterKey);
     g_log->log("SUSPICION about " + suspKey + " from " + reporterKey
              + " (" + std::to_string(entry.confirmers.size())
              + "/" + std::to_string(entry.quorum) + ")");
 
     if ((int)entry.confirmers.size() >= entry.quorum) {
         entry.reported = true;
         g_log->log("PEER-LEVEL CONSENSUS: " + suspKey + " confirmed dead. Reporting to seeds.");
         {
             std::lock_guard<std::mutex> nl(g_neighborMu);
             auto it = g_neighborSocks.find(suspKey);
             if (it != g_neighborSocks.end()) { close(it->second); g_neighborSocks.erase(it); }
         }
         removeNode(suspKey);
         removePongFlag(suspKey);
         reportDeadToSeeds(entry.node);
     }
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Message processing (shared by acceptLoop pre-read and handleIncomingNeighbor)
 // ─────────────────────────────────────────────────────────────────────────────
 
 void processLine(const std::string& line, const std::string& remoteKey, int sock) {
     if (line.rfind(std::string(MSG_GOSSIP) + ":", 0) == 0) {
         std::string payload = line.substr(strlen(MSG_GOSSIP) + 1);
         std::string dedupKey;
         auto parts = split(payload, ':');
         if (parts.size() >= 3) dedupKey = parts[1] + ":" + parts[2];
         else                   dedupKey = hashMsg(payload);
         bool isNew = false;
         {
             std::lock_guard<std::mutex> lk(g_mlMu);
             if (!g_messageList.count(dedupKey)) { g_messageList[dedupKey] = payload; isNew = true; }
         }
         if (isNew) {
             g_log->log("GOSSIP received (first time): " + payload + " from " + remoteKey);
             broadcastGossip(payload, remoteKey);
         }
     }
     else if (line == MSG_PING) {
         // Reply on the same persistent socket — no new connections
         sendAll(sock, std::string(MSG_PONG) + "\n");
     }
     else if (line == MSG_PONG) {
         // Signal the ping thread via atomic flag — no select() needed
         setPongFlag(remoteKey);
     }
     else if (line.rfind(std::string(MSG_SUSPICION) + ":", 0) == 0) {
         auto parts = split(line, ':');
         if (parts.size() >= 5)
             handleSuspicion(parts[1] + ":" + parts[2], parts[3] + ":" + parts[4]);
     }
     else if (line.rfind(MSG_REMOVE_PEER, 0) == 0) {
         auto parts = split(line, ':');
         if (parts.size() >= 3) {
             std::string deadKey = parts[1] + ":" + parts[2];
             g_log->log("Seed confirmed dead node removed: " + deadKey);
             {
                 std::lock_guard<std::mutex> lk(g_neighborMu);
                 auto it = g_neighborSocks.find(deadKey);
                 if (it != g_neighborSocks.end()) { close(it->second); g_neighborSocks.erase(it); }
             }
             removeNode(deadKey);
             removePongFlag(deadKey);
         }
     }
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Persistent neighbor recv loop
 // ─────────────────────────────────────────────────────────────────────────────
 
 void handleIncomingNeighbor(int sock, const std::string& remoteKey) {
     while (true) {
         std::string line = recvLine(sock);
         if (line.empty()) break;
         processLine(line, remoteKey, sock);
     }
 
     // Cleanup on disconnect
     {
         std::lock_guard<std::mutex> lk(g_neighborMu);
         g_neighborSocks.erase(remoteKey);
     }
     removeNode(remoteKey);
     close(sock);
     g_log->log("Neighbor disconnected: " + remoteKey);
 
     // Raise missed-ping count to threshold so pingLoop confirms on next cycle
     bool alreadyReported = false;
     {
         std::lock_guard<std::mutex> ls(g_suspMu);
         auto it = g_suspicions.find(remoteKey);
         if (it != g_suspicions.end() && it->second.reported) alreadyReported = true;
     }
     if (!alreadyReported) {
         std::lock_guard<std::mutex> lp(g_pingMu);
         if (g_missedPings[remoteKey] < MISSED_PINGS_THRESHOLD)
             g_missedPings[remoteKey] = MISSED_PINGS_THRESHOLD;
     }
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Accept loop — every incoming connection is a persistent neighbor
 // ─────────────────────────────────────────────────────────────────────────────
 
 void acceptLoop(int listenSock) {
     while (true) {
         sockaddr_in caddr{};
         socklen_t clen = sizeof(caddr);
         int csock = accept(listenSock, (sockaddr*)&caddr, &clen);
         if (csock < 0) { perror("accept"); continue; }
 
         char ipbuf[INET_ADDRSTRLEN];
         inet_ntop(AF_INET, &caddr.sin_addr, ipbuf, sizeof(ipbuf));
         std::string remoteKey = std::string(ipbuf) + ":" + std::to_string(ntohs(caddr.sin_port));
 
         // First message is HELLO:IP:listenPort — gives us the stable listen port
         std::string hello = recvLine(csock);
         if (hello.rfind("HELLO:", 0) == 0) {
             auto parts = split(hello, ':');
             if (parts.size() >= 3) remoteKey = parts[1] + ":" + parts[2];
         }
 
         // Handle simultaneous mutual connections (both peers connect to each other).
         // DO NOT simply drop the duplicate — the remote peer stored this socket as
         // their outgoing send socket. Closing it silently breaks their sends to us.
         //
         // Tie-breaking rule (consistent on both sides):
         //   Lower-port peer keeps their outgoing (connectNeighbor) socket for sends.
         //   Higher-port peer switches to the incoming socket for sends.
         // Both sockets stay open for receiving.
         bool isDuplicate = false;
         bool spawnMainThread = true;
         {
             std::lock_guard<std::mutex> lk(g_neighborMu);
             if (g_neighborSocks.count(remoteKey)) {
                 isDuplicate = true;
                 int remotePort = std::stoi(split(remoteKey, ':').back());
                 if (g_selfPort > remotePort) {
                     // Remote has lower port — they win the tie, keep their outgoing.
                     // We switch our registered send socket to this incoming one.
                     int oldSock = g_neighborSocks[remoteKey];
                     g_neighborSocks[remoteKey] = csock;
                     close(oldSock);
                     // Fall through: spawn handleIncomingNeighbor on new csock
                 } else {
                     // We have lower port — we keep our outgoing socket.
                     // Spawn a recv-only thread to drain messages on this extra socket.
                     spawnMainThread = false;
                     std::thread([csock, remoteKey]() {
                         while (true) {
                             std::string line = recvLine(csock);
                             if (line.empty()) break;
                             processLine(line, remoteKey, csock);
                         }
                         close(csock);
                     }).detach();
                 }
             } else {
                 g_neighborSocks[remoteKey] = csock;
             }
         }
 
         if (!spawnMainThread) continue;
 
         if (!isDuplicate) {
             registerPongFlag(remoteKey);
             addEdge(g_selfIP + ":" + std::to_string(g_selfPort), remoteKey);
             g_log->log("Neighbor connected: " + remoteKey);
         }
 
         std::thread(handleIncomingNeighbor, csock, remoteKey).detach();
     }
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Connect to a neighbor (outgoing)
 // ─────────────────────────────────────────────────────────────────────────────
 
 bool connectNeighbor(const NodeInfo& nb) {
     std::string key = nb.str();
     {
         std::lock_guard<std::mutex> lk(g_neighborMu);
         if (g_neighborSocks.count(key)) return true;
     }
     int sock = connectTo(nb.ip, nb.port);
     if (sock < 0) { g_log->log("Cannot connect to neighbor " + key); return false; }
 
     sendAll(sock, "HELLO:" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n");
 
     {
         std::lock_guard<std::mutex> lk(g_neighborMu);
         if (g_neighborSocks.count(key)) { close(sock); return true; }
         g_neighborSocks[key] = sock;
     }
     registerPongFlag(key);
     addEdge(g_selfIP + ":" + std::to_string(g_selfPort), key);
     g_log->log("Connected to neighbor " + key);
 
     std::thread(handleIncomingNeighbor, sock, key).detach();
     return true;
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Ping loop — uses persistent socket, no new TCP connections
 // ─────────────────────────────────────────────────────────────────────────────
 
 void pingLoop() {
     while (true) {
         std::this_thread::sleep_for(std::chrono::seconds(PING_INTERVAL_SEC));
 
         // Ensure all current neighbors are in the ping tracking map
         {
             std::lock_guard<std::mutex> ln(g_neighborMu);
             std::lock_guard<std::mutex> lp(g_pingMu);
             for (auto& [k, s] : g_neighborSocks)
                 if (!g_missedPings.count(k)) g_missedPings[k] = 0;
         }
 
         // Collect all tracked peers (includes recently disconnected)
         std::vector<std::string> targets;
         {
             std::lock_guard<std::mutex> lp(g_pingMu);
             for (auto& [k, v] : g_missedPings) targets.push_back(k);
         }
 
         for (auto& key : targets) {
             // Skip already-confirmed dead peers
             {
                 std::lock_guard<std::mutex> ls(g_suspMu);
                 auto it = g_suspicions.find(key);
                 if (it != g_suspicions.end() && it->second.reported) continue;
             }
 
             // Send PING on existing persistent socket — NO new TCP connection
             bool sentPing = false;
             {
                 std::lock_guard<std::mutex> ln(g_neighborMu);
                 auto it = g_neighborSocks.find(key);
                 if (it != g_neighborSocks.end())
                     sentPing = sendAll(it->second, std::string(MSG_PING) + "\n");
             }
 
             if (sentPing) {
                 // Wait for PONG via the atomic flag set by the recv thread.
                 // Sleep for PING_TIMEOUT_SEC then check — clean, no select() race.
                 std::this_thread::sleep_for(std::chrono::seconds(PING_TIMEOUT_SEC));
             }
 
             bool gotPong = consumePong(key);
 
             {
                 std::lock_guard<std::mutex> lp(g_pingMu);
                 if (gotPong) {
                     g_missedPings[key] = 0;
                 } else {
                     g_missedPings[key]++;
                     int missed = g_missedPings[key];
                     if (missed >= MISSED_PINGS_THRESHOLD) {
                         g_log->log("SUSPICION: " + key + " has missed "
                                  + std::to_string(missed) + " pings. Broadcasting suspicion.");
                         broadcastSuspicion(key);
                         handleSuspicion(key, g_selfIP + ":" + std::to_string(g_selfPort));
                     }
                 }
             }
         }
     }
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Gossip generation loop — every 5 seconds, max 10 messages
 // ─────────────────────────────────────────────────────────────────────────────
 
 void gossipLoop() {
     std::this_thread::sleep_for(std::chrono::seconds(2));
     while (g_msgCounter < MAX_GOSSIP_MSGS) {
         int msgNum = ++g_msgCounter;
         std::string ts      = std::to_string(epochSec());
         std::string payload = ts + ":" + g_selfIP + ":" + std::to_string(msgNum);
         std::string dedupKey = g_selfIP + ":" + std::to_string(msgNum);
         {
             std::lock_guard<std::mutex> lk(g_mlMu);
             g_messageList[dedupKey] = payload;
         }
         broadcastGossip(payload);
         g_log->log("GOSSIP sent [" + std::to_string(msgNum) + "]: " + payload);
         std::this_thread::sleep_for(std::chrono::seconds(GOSSIP_INTERVAL_SEC));
     }
     g_log->log("Gossip generation complete (sent " + std::to_string(MAX_GOSSIP_MSGS) + " messages).");
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Seed interaction
 // ─────────────────────────────────────────────────────────────────────────────
 
 bool registerWithSeed(const NodeInfo& seed) {
     int sock = connectTo(seed.ip, seed.port, SEED_CONNECT_TIMEOUT);
     if (sock < 0) { g_log->log("Cannot reach seed " + seed.str()); return false; }
     sendAll(sock, "REGISTER:" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n");
     std::string resp = recvLine(sock);
     bool ok = (resp.rfind(MSG_REGISTER_OK, 0) == 0);
     g_log->log(ok ? "Registered with seed " + seed.str()
                   : "Registration failed with seed " + seed.str());
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
         for (auto& p : split(list, ',')) {
             NodeInfo n = parseNode(p);
             if (!n.ip.empty()) peers.push_back(n);
         }
         g_log->log("Peer list from seed " + seed.str() + ": " + list);
     }
     return peers;
 }
 
 // ─────────────────────────────────────────────────────────────────────────────
 //  Main
 // ─────────────────────────────────────────────────────────────────────────────
 
 int main(int argc, char* argv[]) {
     if (argc < 2) { std::cerr << "Usage: ./peer <port> [config.txt]\n"; return 1; }
 
     g_selfPort = std::stoi(argv[1]);
     char* envIP = getenv("SELF_IP");
     g_selfIP = envIP ? envIP : "127.0.0.1";
 
     std::string configPath = (argc >= 3) ? argv[2] : "config.txt";
     g_allSeeds = readConfig(configPath);
     g_numSeeds = (int)g_allSeeds.size();
     if (g_numSeeds == 0) { std::cerr << "[ERROR] No seeds in config.\n"; return 1; }
 
     std::string logFile = "peer_" + std::to_string(g_selfPort) + "_output.txt";
     g_log = new Logger(logFile);
     g_log->log("Peer started at " + g_selfIP + ":" + std::to_string(g_selfPort)
              + " | seeds=" + std::to_string(g_numSeeds));
 
     int listenSock = createListenSocket(g_selfPort);
     if (listenSock < 0) return 1;
     std::thread(acceptLoop, listenSock).detach();
 
     // Phase 1: Register with quorum of seeds
     int required = g_numSeeds / 2 + 1;
     g_log->log("Need to register with " + std::to_string(required)
              + " of " + std::to_string(g_numSeeds) + " seeds.");
 
     std::vector<NodeInfo> shuffled = g_allSeeds;
     std::shuffle(shuffled.begin(), shuffled.end(), std::mt19937{std::random_device{}()});
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
         close(listenSock); delete g_log; return 1;
     }
     g_log->log("Successfully registered with " + std::to_string(registered) + " seeds.");
 
     // Phase 2: Fetch peer lists and select neighbors
     std::set<NodeInfo> allPeers;
     {
         std::lock_guard<std::mutex> lk(g_seedMu);
         for (auto& seed : g_registeredSeeds)
             for (auto& p : getPeerListFromSeed(seed)) allPeers.insert(p);
     }
     allPeers.erase(NodeInfo{g_selfIP, g_selfPort});
     std::vector<NodeInfo> candidates(allPeers.begin(), allPeers.end());
     g_log->log("Total candidate peers: " + std::to_string(candidates.size()));
 
     std::vector<NodeInfo> neighbors = selectNeighbors(candidates);
     g_log->log("Selected " + std::to_string(neighbors.size()) + " neighbors.");
 
     // Phase 3: Connect to selected neighbors
     for (auto& nb : neighbors) {
         connectNeighbor(nb);
         std::this_thread::sleep_for(std::chrono::milliseconds(200));
     }
 
     // Phase 4: Start gossip and ping loops
     std::thread gossipThread(gossipLoop);
     std::thread pingThread(pingLoop);
 
     gossipThread.join();
     pingThread.detach();
 
     g_log->log("Gossip phase complete. Continuing liveness monitoring.");
     while (true) std::this_thread::sleep_for(std::chrono::seconds(60));
 
     close(listenSock);
     delete g_log;
     return 0;
 }