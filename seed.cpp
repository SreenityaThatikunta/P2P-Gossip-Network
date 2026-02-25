#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <map>
#include <set>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fstream>
#include <sstream>

using namespace std;

struct PeerInfo {
    string ip;
    int port;
    int degree;
};

map<string, int> peerList; // "IP:Port" -> Degree
map<string, set<string>> deadReports; // "DeadIP:Port" -> Set of reporters
mutex mtx;

void handle_client(int client_socket) {
    char buffer[1024] = {0};
    read(client_socket, buffer, 1024);
    string request(buffer);
    stringstream ss(request);
    string cmd;
    ss >> cmd;

    if (cmd == "Register:") {
        string ip, port;
        ss >> ip >> port;
        string peer_addr = ip + ":" + port;
        
        lock_guard<mutex> lock(mtx);
        peerList[peer_addr] = 0;
        cout << "[CONSENSUS] Peer registered: " << peer_addr << endl;
        send(client_socket, "Registered", 10, 0);
    } 
    else if (cmd == "RequestList") {
        lock_guard<mutex> lock(mtx);
        string list_msg;
        for (auto const& [addr, deg] : peerList) {
            list_msg += addr + "|" + to_string(deg) + ",";
        }
        send(client_socket, list_msg.c_str(), list_msg.length(), 0);
    }
    else if (cmd == "DeadNode:") {
        string deadIP, deadPort, reporterIP, reporterPort;
        ss >> deadIP >> deadPort >> reporterIP >> reporterPort;
        string dead_addr = deadIP + ":" + deadPort;
        string reporter = reporterIP + ":" + reporterPort;

        lock_guard<mutex> lock(mtx);
        deadReports[dead_addr].insert(reporter);
        
        // Consensus: Require at least 2 independent reports to remove
        if (deadReports[dead_addr].size() >= 2) {
            peerList.erase(dead_addr);
            cout << "[REMOVAL] Dead node confirmed and removed: " << dead_addr << endl;
        }
    }

    close(client_socket);
}

int main(int argc, char const *argv[]) {
    if (argc < 3) { cout << "Usage: ./seed <IP> <Port>" << endl; return 1; }
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(argv[1]);
    address.sin_port = htons(stoi(argv[2]));

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 10);
    cout << "[SEED] Running on " << argv[1] << ":" << argv[2] << endl;

    while (true) {
        int new_socket = accept(server_fd, NULL, NULL);
        thread(handle_client, new_socket).detach();
    }
    return 0;
}