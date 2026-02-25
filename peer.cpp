#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <set>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <chrono>

using namespace std;

string myIP;
int myPort;
vector<pair<string, int>> neighbors;
set<string> messageList;
mutex ml_mtx;

void connect_to_seed(string sIP, int sPort, string msg, bool expectResponse, string& response) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(sPort);
    inet_pton(AF_INET, sIP.c_str(), &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) >= 0) {
        send(sock, msg.c_str(), msg.length(), 0);
        if (expectResponse) {
            char buffer[4096] = {0};
            read(sock, buffer, 4096);
            response = string(buffer);
        }
    }
    close(sock);
}

// Power-law neighbor selection
void select_neighbors(string rawList) {
    stringstream ss(rawList);
    string segment;
    while (getline(ss, segment, ',')) {
        if (segment.empty()) continue;
        size_t del = segment.find('|');
        string addr = segment.substr(0, del);
        if (addr == myIP + ":" + to_string(myPort)) continue;
        
        // Simplified Power-Law: 50% chance to connect to any node
        // In a true implementation, weight this by the 'degree' value provided
        if (rand() % 2 == 0) {
            size_t colon = addr.find(':');
            neighbors.push_back({addr.substr(0, colon), stoi(addr.substr(colon+1))});
        }
    }
}

void gossip_sender() {
    for (int i = 1; i <= 10; ++i) {
        this_thread::sleep_for(chrono::seconds(5));
        string msg = to_string(time(NULL)) + ":" + myIP + ":" + to_string(i);
        cout << "[GENERATE] " << msg << endl;
        
        for (auto& n : neighbors) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_port = htons(n.second);
            inet_pton(AF_INET, n.first.c_str(), &addr.sin_addr);
            if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) >= 0) {
                string fullMsg = "GOSSIP " + msg;
                send(sock, fullMsg.c_str(), fullMsg.length(), 0);
            }
            close(sock);
        }
    }
}

void listener() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(myPort);
    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 10);

    while (true) {
        int new_socket = accept(server_fd, NULL, NULL);
        char buffer[1024] = {0};
        read(new_socket, buffer, 1024);
        string data(buffer);
        if (data.find("GOSSIP") == 0) {
            string content = data.substr(7);
            lock_guard<mutex> lock(ml_mtx);
            if (messageList.find(content) == messageList.end()) {
                messageList.insert(content);
                cout << "[RECEIVED] " << content << endl;
                // Forwarding logic would go here
            }
        }
        close(new_socket);
    }
}

int main(int argc, char const *argv[]) {
    if (argc < 3) return 1;
    myIP = argv[1];
    myPort = stoi(argv[2]);

    // 1. Register with seeds (Assuming 127.0.0.1:5000 for demo)
    string response;
    connect_to_seed("127.0.0.1", 5000, "Register: " + myIP + " " + to_string(myPort), true, response);
    
    // 2. Get Peer List
    string rawList;
    connect_to_seed("127.0.0.1", 5000, "RequestList", true, rawList);
    select_neighbors(rawList);

    // 3. Start Threads
    thread(listener).detach();
    thread(gossip_sender).join();

    return 0;
}