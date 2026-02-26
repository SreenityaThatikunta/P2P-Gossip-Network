#include "common.h"

static std::string g_selfIP;
static int g_selfPort = 0;
static Logger *g_log = nullptr;

static std::set<NodeInfo> g_peerList;
static std::mutex g_plMu;

static std::vector<NodeInfo> g_seeds;
static int g_numSeeds = 0;

struct RegProposal
{
    NodeInfo peer;
    int yesVotes = 1;
    int noVotes = 0;
    bool committed = false;
    std::set<std::string> voters;
};
static std::map<std::string, RegProposal> g_regProposals;
static std::mutex g_regMu;

struct DeadProposal
{
    NodeInfo dead;
    int yesVotes = 1;
    int noVotes = 0;
    bool committed = false;
    std::set<std::string> voters;
};
static std::map<std::string, DeadProposal> g_deadProposals;
static std::mutex g_deadMu;

static std::map<std::string, int> g_peerSocks;
static std::mutex g_peerSockMu;

int quorum()
{
    return g_numSeeds / 2 + 1;
}

std::string peerListStr()
{
    std::lock_guard<std::mutex> lk(g_plMu);
    std::string s;
    for (auto &p : g_peerList)
    {
        if (!s.empty())
            s += ",";
        s += p.str();
    }
    return s;
}

void broadcastToSeeds(const std::string &msg)
{
    for (auto &seed : g_seeds)
    {
        int s = connectTo(seed.ip, seed.port, 2);
        if (s < 0)
            continue;
        sendAll(s, msg + "\n");
        close(s);
    }
}

void notifyPeersRemoval(const NodeInfo &dead)
{
    std::string msg = std::string(MSG_REMOVE_PEER) + ":" + dead.str() + "\n";
    std::lock_guard<std::mutex> lk(g_peerSockMu);
    for (auto &[key, sock] : g_peerSocks)
    {
        sendAll(sock, msg);
    }
}

void proposeRegistration(const NodeInfo &peer)
{
    std::string key = peer.str();

    {
        std::lock_guard<std::mutex> lk(g_regMu);
        if (g_regProposals.count(key) && g_regProposals[key].committed)
            return;
        RegProposal &rp = g_regProposals[key];
        rp.peer = peer;
        rp.yesVotes = 1;
        rp.voters.insert(g_selfIP + ":" + std::to_string(g_selfPort));
    }

    g_log->log("REG_PROPOSAL: Proposing registration of peer " + key);

    std::string msg = std::string(MSG_REGISTER_PROPOSAL) + ":" + key + ":" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n";
    broadcastToSeeds(msg);

    if (g_numSeeds == 1)
    {
        std::lock_guard<std::mutex> lk(g_regMu);
        auto &rp = g_regProposals[key];
        if (!rp.committed)
        {
            rp.committed = true;
            {
                std::lock_guard<std::mutex> pl(g_plMu);
                g_peerList.insert(peer);
            }
            g_log->log("REG_COMMIT (single-seed): Peer " + key + " registered.");
        }
    }
}

void handleRegVote(const std::string &peerKey, const std::string &voterKey, bool yes)
{
    std::lock_guard<std::mutex> lk(g_regMu);
    auto it = g_regProposals.find(peerKey);
    if (it == g_regProposals.end())
        return;
    RegProposal &rp = it->second;
    if (rp.committed)
        return;
    if (rp.voters.count(voterKey))
        return;
    rp.voters.insert(voterKey);
    if (yes)
        rp.yesVotes++;
    else
        rp.noVotes++;

    g_log->log("REG_VOTE: peer=" + peerKey + " yes=" + std::to_string(rp.yesVotes) + " no=" + std::to_string(rp.noVotes));

    if (rp.yesVotes >= quorum() && !rp.committed)
    {
        rp.committed = true;
        NodeInfo p = rp.peer;
        {
            std::lock_guard<std::mutex> pl(g_plMu);
            g_peerList.insert(p);
        }
        g_log->log("REG_COMMIT: Peer " + peerKey + " registered after consensus.");
        std::string cm = std::string(MSG_REGISTER_COMMIT) + ":" + peerKey + "\n";
        broadcastToSeeds(cm);
    }
}

void proposeDeadNode(const NodeInfo &dead)
{
    std::string key = dead.str();

    {
        std::lock_guard<std::mutex> lk(g_deadMu);
        if (g_deadProposals.count(key) && g_deadProposals[key].committed)
            return;
        DeadProposal &dp = g_deadProposals[key];
        dp.dead = dead;
        dp.yesVotes = 1;
        dp.voters.insert(g_selfIP + ":" + std::to_string(g_selfPort));
    }

    g_log->log("DEAD_PROPOSAL: Proposing removal of dead peer " + key);

    std::string msg = std::string(MSG_DEAD_PROPOSAL) + ":" + key + ":" + g_selfIP + ":" + std::to_string(g_selfPort) + "\n";
    broadcastToSeeds(msg);

    if (g_numSeeds == 1)
    {
        std::lock_guard<std::mutex> lk(g_deadMu);
        auto &dp = g_deadProposals[key];
        if (!dp.committed)
        {
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

void handleDeadVote(const std::string &deadKey, const std::string &voterKey, bool yes)
{
    std::lock_guard<std::mutex> lk(g_deadMu);
    auto it = g_deadProposals.find(deadKey);
    if (it == g_deadProposals.end())
        return;
    DeadProposal &dp = it->second;
    if (dp.committed)
        return;
    if (dp.voters.count(voterKey))
        return;
    dp.voters.insert(voterKey);
    if (yes)
        dp.yesVotes++;
    else
        dp.noVotes++;

    g_log->log("DEAD_VOTE: dead=" + deadKey + " yes=" + std::to_string(dp.yesVotes));

    if (dp.yesVotes >= quorum() && !dp.committed)
    {
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

void handleConnection(int clientSock, std::string clientIP)
{
    while (true)
    {
        std::string line = recvLine(clientSock);
        if (line.empty())
            break;

        if (line.rfind(MSG_REGISTER_PROPOSAL, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 5)
                continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            std::string propKey = parts[3] + ":" + parts[4];

            NodeInfo peer = parseNode(peerKey);
            {
                std::lock_guard<std::mutex> lk(g_regMu);
                if (!g_regProposals.count(peerKey))
                {
                    RegProposal &rp = g_regProposals[peerKey];
                    rp.peer = peer;
                    rp.yesVotes = 0;
                    rp.voters.insert(propKey);
                }
            }

            g_log->log("RECV REGISTER_PROPOSAL for " + peerKey + " from " + propKey);

            bool voteYes = true;
            std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
            std::string vote = std::string(MSG_REGISTER_VOTE) + ":" + peerKey + ":" + myKey + ":" + (voteYes ? "YES" : "NO") + "\n";

            int s = connectTo(parts[3], std::stoi(parts[4]), 2);
            if (s >= 0)
            {
                sendAll(s, vote);
                close(s);
            }

            handleRegVote(peerKey, myKey, voteYes);
        }

        else if (line.rfind(MSG_REGISTER_VOTE, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 6)
                continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            std::string voterKey = parts[3] + ":" + parts[4];
            bool yes = (parts[5] == "YES");
            handleRegVote(peerKey, voterKey, yes);
        }

        else if (line.rfind(MSG_REGISTER_COMMIT, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 3)
                continue;
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

        else if (line.rfind("REGISTER:", 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 3)
                continue;
            std::string peerKey = parts[1] + ":" + parts[2];
            NodeInfo peer = parseNode(peerKey);

            {
                std::lock_guard<std::mutex> pl(g_plMu);
                if (g_peerList.count(peer))
                {
                    sendAll(clientSock, std::string(MSG_REGISTER_OK) + ":" + peerKey + "\n");
                    continue;
                }
            }

            g_log->log("REGISTER request from peer " + peerKey);

            {
                std::lock_guard<std::mutex> lk(g_peerSockMu);
                g_peerSocks[peerKey] = clientSock;
            }

            proposeRegistration(peer);

            bool ok = false;
            for (int i = 0; i < 100; i++)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                std::lock_guard<std::mutex> lk(g_regMu);
                auto it = g_regProposals.find(peerKey);
                if (it != g_regProposals.end() && it->second.committed)
                {
                    ok = true;
                    break;
                }
            }

            if (ok)
            {
                sendAll(clientSock, std::string(MSG_REGISTER_OK) + ":" + peerKey + "\n");
                g_log->log("Sent REGISTER_OK to peer " + peerKey);
            }
            else
            {
                sendAll(clientSock, std::string(MSG_REGISTER_FAIL) + ":" + peerKey + "\n");
                g_log->log("Sent REGISTER_FAIL to peer " + peerKey);
            }
        }

        else if (line == "GET_PEER_LIST")
        {
            std::string pl = MSG_PEER_LIST + std::string(":") + peerListStr() + "\n";
            sendAll(clientSock, pl);
            g_log->log("Sent PEER_LIST to client on socket " + std::to_string(clientSock));
        }

        else if (line.rfind(MSG_DEAD_REPORT, 0) == 0)
        {
            size_t p1 = line.find(':');
            if (p1 == std::string::npos)
                continue;
            std::string rest = line.substr(p1 + 1);
            auto parts = split(rest, ':');
            if (parts.size() < 2)
                continue;
            std::string deadKey = parts[0] + ":" + parts[1];
            NodeInfo dead = parseNode(deadKey);

            g_log->log("Received Dead Node report for " + deadKey + " from " + clientIP);

            proposeDeadNode(dead);
        }

        else if (line.rfind(MSG_DEAD_PROPOSAL, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 5)
                continue;
            std::string deadKey = parts[1] + ":" + parts[2];
            std::string propKey = parts[3] + ":" + parts[4];
            NodeInfo dead = parseNode(deadKey);

            {
                std::lock_guard<std::mutex> lk(g_deadMu);
                if (!g_deadProposals.count(deadKey))
                {
                    DeadProposal &dp = g_deadProposals[deadKey];
                    dp.dead = dead;
                    dp.yesVotes = 0;
                    dp.voters.insert(propKey);
                }
            }
            std::string myKey = g_selfIP + ":" + std::to_string(g_selfPort);
            std::string vote = std::string(MSG_DEAD_VOTE) + ":" + deadKey + ":" + myKey + ":YES\n";
            int s = connectTo(parts[3], std::stoi(parts[4]), 2);
            if (s >= 0)
            {
                sendAll(s, vote);
                close(s);
            }
            handleDeadVote(deadKey, myKey, true);
        }

        else if (line.rfind(MSG_DEAD_VOTE, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 6)
                continue;
            std::string deadKey = parts[1] + ":" + parts[2];
            std::string voterKey = parts[3] + ":" + parts[4];
            bool yes = (parts[5] == "YES");
            handleDeadVote(deadKey, voterKey, yes);
        }

        else if (line.rfind(MSG_DEAD_COMMIT, 0) == 0)
        {
            auto parts = split(line, ':');
            if (parts.size() < 3)
                continue;
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
    }

    {
        std::lock_guard<std::mutex> lk(g_peerSockMu);
        for (auto it = g_peerSocks.begin(); it != g_peerSocks.end();)
        {
            if (it->second == clientSock)
                it = g_peerSocks.erase(it);
            else
                ++it;
        }
    }
    close(clientSock);
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: ./seed <port> [config.txt]\n";
        return 1;
    }

    g_selfPort = std::stoi(argv[1]);

    char *envIP = getenv("SELF_IP");
    g_selfIP = envIP ? envIP : "127.0.0.1";

    std::string configPath = (argc >= 3) ? argv[2] : "config.txt";
    std::vector<NodeInfo> allSeeds = readConfig(configPath);
    g_numSeeds = (int)allSeeds.size();

    if (g_numSeeds == 0)
    {
        std::cerr << "[ERROR] No seeds found in config.\n";
        return 1;
    }

    NodeInfo self{g_selfIP, g_selfPort};
    for (auto &s : allSeeds)
    {
        if (!(s == self))
            g_seeds.push_back(s);
    }

    std::string logFile = "seed_" + std::to_string(g_selfPort) + "_output.txt";
    g_log = new Logger(logFile);
    g_log->log("Seed node started at " + self.str() + " | total seeds=" + std::to_string(g_numSeeds) + " | quorum=" + std::to_string(quorum()));

    int listenSock = createListenSocket(g_selfPort);
    if (listenSock < 0)
        return 1;

    g_log->log("Listening on port " + std::to_string(g_selfPort));

    while (true)
    {
        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int csock = accept(listenSock, (sockaddr *)&caddr, &clen);
        if (csock < 0)
        {
            perror("accept");
            continue;
        }

        char ipbuf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &caddr.sin_addr, ipbuf, sizeof(ipbuf));
        std::string cip(ipbuf);

        std::thread(handleConnection, csock, cip).detach();
    }

    close(listenSock);
    delete g_log;
    return 0;
}
