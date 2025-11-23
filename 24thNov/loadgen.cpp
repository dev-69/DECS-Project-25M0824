#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

// ================= CONSTANTS =================
const char* HOST = "127.0.0.1";
const int PORT = 6969;

const int TEST_DURATION = 30;
const int POPULAR_RANGE = 50;
const int MAX_KEY = 10000000;

// ================= SIMPLE RNG =================
unsigned int simpleRand(unsigned int& seed) {
    seed = seed * 1103515245 + 12345;
    return (seed >> 16) & 0x7FFF;
}

// ================= PAYLOAD GENERATOR =================
string generatePayload(const string& mode, const string& connHeader, unsigned int& seed) {
    int key = (simpleRand(seed) % MAX_KEY) + 1;
    string header = "HTTP/1.1\r\nConnection: " + connHeader + "\r\n\r\n";

    if (mode == "GET_POPULAR") {
        int k = (simpleRand(seed) % POPULAR_RANGE);
        return "GET /get?key=" + to_string(k) + " " + header;
    }
    if (mode == "GET_ALL") {
        return "GET /get?key=" + to_string(key) + " " + header;
    }
    if (mode == "PUT_ALL") {
        string val(512, 'X');
        return "GET /set?key=" + to_string(key) + "&value=" + val + " " + header;
    }
    if (mode == "GET_PUT_MIX") {
        int r = simpleRand(seed) % 100;

        if (r < 50)
            return "GET /get?key=" + to_string(key) + " " + header;
        else if (r < 90)
            return "GET /set?key=" + to_string(key) + "&value=mix_" + to_string(simpleRand(seed) % 1024) + " " + header;
        else
            return "GET /delete?key=" + to_string(key) + " " + header;
    }
    return "";
}

// ================= WORKER (KEEP-ALIVE) =================
void workerKeepAlive(const string& mode, int duration,
                     long long& reqOut, double& latencyOut)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return;

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &server.sin_addr);

    if (connect(sockfd, (sockaddr*)&server, sizeof(server)) < 0) {
        close(sockfd);
        return;
    }

    unsigned int seed = chrono::high_resolution_clock::now().time_since_epoch().count();

    long long reqCount = 0;
    double latencySum = 0;

    auto start = chrono::high_resolution_clock::now();

    while (true) {
        auto now = chrono::high_resolution_clock::now();
        if (chrono::duration<double>(now - start).count() >= duration) break;

        string payload = generatePayload(mode, "keep-alive", seed);

        auto t0 = chrono::high_resolution_clock::now();
        send(sockfd, payload.c_str(), payload.size(), 0);

        char buffer[4096];
        recv(sockfd, buffer, sizeof(buffer), 0);
        auto t1 = chrono::high_resolution_clock::now();

        reqCount++;
        latencySum += chrono::duration<double>(t1 - t0).count();
    }

    close(sockfd);

    reqOut = reqCount;
    latencyOut = latencySum;
}

// ================= WORKER (SHORT-LIVED) =================
void workerShort(const string& mode, int duration,
                 long long& reqOut, double& latencyOut)
{
    unsigned int seed = chrono::high_resolution_clock::now().time_since_epoch().count();

    long long reqCount = 0;
    double latencySum = 0;

    auto start = chrono::high_resolution_clock::now();

    while (true) {
        auto now = chrono::high_resolution_clock::now();
        if (chrono::duration<double>(now - start).count() >= duration) break;

        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) continue;

        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(PORT);
        inet_pton(AF_INET, HOST, &server.sin_addr);

        if (connect(sockfd, (sockaddr*)&server, sizeof(server)) < 0) {
            close(sockfd);
            continue;
        }

        string payload = generatePayload(mode, "close", seed);

        auto t0 = chrono::high_resolution_clock::now();
        send(sockfd, payload.c_str(), payload.size(), 0);

        char buffer[4096];
        recv(sockfd, buffer, sizeof(buffer), 0);
        auto t1 = chrono::high_resolution_clock::now();

        close(sockfd);

        reqCount++;
        latencySum += chrono::duration<double>(t1 - t0).count();
    }

    reqOut = reqCount;
    latencyOut = latencySum;
}

// ================= MAIN =================
int main(int argc, char* argv[]) {
    if (argc != 4) {
        cout << "Usage: ./loadgen <WORKLOAD> <CONN> <CLIENTS>\n";
        cout << "WORKLOAD: GET_POPULAR | GET_ALL | PUT_ALL | GET_PUT_MIX\n";
        cout << "CONN: KEEP_ALIVE | CLOSE\n";
        return 1;
    }

    string mode = argv[1];
    string conn = argv[2];
    int numClients = stoi(argv[3]);

    cout << "Running benchmark:\n";
    cout << "  Workload: " << mode << "\n";
    cout << "  Connection: " << conn << "\n";
    cout << "  Clients: " << numClients << "\n\n";

    vector<long long> reqs(numClients, 0);
    vector<double> lats(numClients, 0.0);

    vector<thread> threads;

    for (int i = 0; i < numClients; i++) {
        if (conn == "KEEP_ALIVE")
            threads.emplace_back(workerKeepAlive, mode, TEST_DURATION,
                                 ref(reqs[i]), ref(lats[i]));
        else
            threads.emplace_back(workerShort, mode, TEST_DURATION,
                                 ref(reqs[i]), ref(lats[i]));
    }

    for (auto& t : threads) t.join();

    // Aggregate
    long long totalReq = 0;
    double totalLat = 0;

    for (int i = 0; i < numClients; i++) {
        totalReq += reqs[i];
        totalLat += lats[i];
    }

    double throughput = totalReq / (double)TEST_DURATION;
    double avgLatency = (totalReq == 0 ? 0 : (totalLat / totalReq) * 1000.0);

    cout << "=====================\n";
    cout << "Total Requests: " << totalReq << "\n";
    cout << "Throughput:     " << throughput << " req/s\n";
    cout << "Avg Latency:    " << avgLatency << " ms\n";

    return 0;
}
