#include <iostream>
#include <unistd.h>
#include <string.h>
#include <cstring>
#include <vector>
#include <map>
#include <sstream>
#include <thread>
#include <mutex>
#include <csignal>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <queue>
#include <condition_variable>
#include <algorithm>
#include <atomic>

#define FRONTEND_PORT 6969
#define BACKEND_IP "127.0.0.1" 
#define BACKEND_PORT 7000      

#define BUFFER_SIZE 10240

const int NUM_THREADS = 8;
#define N 100

std::atomic<long> g_total_access(0);
std::atomic<long> g_cache_hits(0);

volatile sig_atomic_t g_shutdown_flag = 0;
std::mutex g_store_mutex;

int g_backend_sock = -1;
std::mutex g_backend_mutex;

int count_of_pairs = 0;

std::vector<int> g_active_sockets;
std::mutex g_active_socket_list_mutex;
int g_server_fd = -1;

void heavy_computation() 
{
    volatile long count = 0;
    for (long i = 0; i < 100000; i++) 
    {
        count += (i * i) % 100;
    }
}

void add_socket(int sock)
{
    std::lock_guard<std::mutex> lock(g_active_socket_list_mutex);
    g_active_sockets.push_back(sock);
}

void remove_socket(int sock)
{
    std::lock_guard<std::mutex> lock(g_active_socket_list_mutex);
    auto it = std::remove(g_active_sockets.begin(), g_active_sockets.end(), sock);

    g_active_sockets.erase(it, g_active_sockets.end());
    
}

std::string urlEncode(const std::string& str)
{
    std::string encoded;
    for(char c : str) 
    {
        if(isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
            encoded += c;
        else 
        {
            encoded += '%';
            char hex[3];
            sprintf(hex, "%02X", static_cast<unsigned char>(c));
            encoded += hex;
        }
    }
    return encoded;
}

std::string urlDecode(const std::string &str) 
{
    std::string decoded;
    char hex[3] = {0};
    for(size_t i = 0; i < str.length(); i++) 
    {
        if(str[i] == '%') 
        {
            if(i + 2 < str.length()) 
            {
                hex[0] = str[i + 1];
                hex[1] = str[i + 2];
                decoded += static_cast<char>(strtol(hex, nullptr, 16));
                i += 2;
            }
        } 
        else if(str[i] == '+') 
        {
            decoded += ' ';
        } else 
        {
            decoded += str[i];
        }
    }
    return decoded;
}

std::string getResponseBody(const std::string& response) 
{
    size_t body_start = response.find("\r\n\r\n");
    if(body_start != std::string::npos) 
    {
        return response.substr(body_start+4);
    }
    return "Error: Malformed Backend Response.";
}

class Node 
{
    public:
        std::string key, value;
        bool dirty = false;
        Node* prev;
        Node* next;

        Node(std::string k, std::string v) : key(k), value(v), dirty(false), prev(nullptr), next(nullptr) {}

        void moveToFront(Node* head) {
            if(head->next == this) return;
            this->prev->next = this->next;
            this->next->prev = this->prev;
            this->next = head->next;
            this->prev = head;
            head->next->prev = this;
            head->next = this;
        }
};

Node* head = new Node("-1", "-1");
Node* tail = new Node("-1", "-1");

void detachNode(Node *node) 
{
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

void attachToFront(Node *node) 
{
    node->next = head->next;
    node->prev = head;
    head->next->prev = node;
    head->next = node;
}

using KeyValueStore = std::map<std::string, Node*>;

class ThreadSafeQueue 
{
    private:
        std::queue<int> m_queue;
        std::mutex m_mutex;
        std::condition_variable m_cond;
        bool m_stop = false;
    public:
        void push(int task) 
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_queue.push(task);
            m_cond.notify_one();
        }
        int pop() 
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            while(m_stop == false && m_queue.empty()) 
            {
                m_cond.wait(lock);
            }
            if(m_stop && m_queue.empty()) return -1;
            int task = m_queue.front();
            m_queue.pop();
            return task;
        }
        void stop() 
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stop = true;
            m_cond.notify_all();
        }
};

bool connectToBackend() 
{
    g_backend_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (g_backend_sock < 0) 
    {
        perror("ERROR: Failed to create socket for Backend connection.");
        return false;
    }

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(BACKEND_PORT);

    if (inet_pton(AF_INET, BACKEND_IP, &server_address.sin_addr) <= 0) 
    {
        perror("ERROR: Invalid Backend address.");
        close(g_backend_sock);
        g_backend_sock = -1;
        return false;
    }

    if (connect(g_backend_sock, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) 
    {
        perror("ERROR: Failed to connect to Backend DB Server.");
        close(g_backend_sock);
        g_backend_sock = -1;
        return false;
    }

    std::cout << "[INFO] Successfully established persistent connection to Backend DB." << std::endl;
    return true;
}

std::string sendToBackend(const std::string& path_and_query, std::string& http_status)
{
    std::lock_guard<std::mutex> lock(g_backend_mutex);

    if (g_backend_sock == -1) 
    {
        http_status = "503 Service Unavailable";
        return "ERROR: Backend connection is closed or failed to initialize.";
    }

    std::string http_request = "GET " + path_and_query + " HTTP/1.1\r\nConnection: keep-alive\r\n\r\n";
    
    if (send(g_backend_sock, http_request.c_str(), http_request.length(), 0) < 0) 
    {
        perror("ERROR: Send to Backend failed");
        http_status = "503 Service Unavailable";
        return "ERROR: Failed to send data to Backend DB Server.";
    }

    char buffer[BUFFER_SIZE] = {0};
    std::string full_response;
    int bytes_read = read(g_backend_sock, buffer, BUFFER_SIZE - 1);
    
    if (bytes_read <= 0) 
    {
        perror("ERROR: Read from Backend failed or connection closed");
        http_status = "503 Service Unavailable";
        return "ERROR: Failed to read response from Backend DB Server.";
    }

    full_response.append(buffer, bytes_read);

    size_t status_start = full_response.find("HTTP/1.1 ");
    size_t status_end = full_response.find("\r\n");
    if (status_start != std::string::npos && status_end != std::string::npos) 
    {
        http_status = full_response.substr(status_start + 9, status_end - (status_start + 9));
    } 
    else 
    {
        http_status = "500 Internal Server Error";
        return "Error: Malformed response from Backend.";
    }

    return getResponseBody(full_response);
}

void writeToBackendDB(Node *node, std::string& http_status)
{
    if(!node->dirty) return;

    std::cout << "[INFO] Writing dirty key to Backend DB on eviction/flush: " << node->key << std::endl;

    std::string path_and_query = "/db_set?key=" + urlEncode(node->key) + "&value=" + urlEncode(node->value);

    std::string backend_response = sendToBackend(path_and_query, http_status);

    if (http_status.rfind("200 OK", 0) == 0)
    {
        node->dirty = false;
    } else 
    {
        std::cerr << "[ERROR] Backend Write Failed (" << http_status << "): " << backend_response << std::endl;
    }
}

std::string handle_set(const std::string& query, KeyValueStore& store, std::string& http_status)
{
    g_total_access++;
    std::lock_guard<std::mutex> lock(g_store_mutex);

    size_t keyPos = query.find("key=");
    size_t valPos = query.find("value=");

    if(keyPos == std::string::npos || valPos == std::string::npos) 
    {
        http_status = "400 Bad Request";
        return "Error missing 'key' or 'value' parameter for /set.";
    }

    keyPos += 4;
    valPos += 6;

    std::string key = urlDecode(query.substr(keyPos, query.find("&", keyPos) - keyPos));
    std::string value = urlDecode(query.substr(valPos));

    auto it = store.find(key);

    if(it != store.end())
    {
        g_cache_hits++;
        Node* foundNode = it->second;
        std::cout << "[INFO] Cache Hit for Set. Updating Key: " << key << std::endl;

        foundNode->value = value;
        foundNode->dirty = true; 
        foundNode->moveToFront(head);
    }
    else
    {
        //std::cout << "[INFO] Cache MISS for SET. Adding Key: " << key << std::endl;

        if(count_of_pairs == N)
        {
            Node* node_to_evict = tail->prev;
            //std::cout << "[INFO] Cache full. Evicting LRU key: " << node_to_evict->key << std::endl;
            writeToBackendDB(node_to_evict, http_status);
            detachNode(node_to_evict);
            store.erase(node_to_evict->key);
            delete node_to_evict;
            count_of_pairs--;
        }

        Node* newNode = new Node(key, value);
        newNode->dirty = true;
        attachToFront(newNode);
        store[key] = newNode;
        count_of_pairs++;
    }

    //std::cout << "[LOG] Set Key " << key << " to " << value << " (in cache and marked dirty)" << std::endl;
    return "OK: Key " + key + " was set (in cache and marked dirty)";
}

std::string handle_get(const std::string& query, KeyValueStore& store, std::string& http_status)
{
    g_total_access++;
    std::string value_copy;
    bool found = false;

    size_t keyPos = query.find("key=");
    if(keyPos == std::string::npos) 
    {
        http_status = "400 Bad Request";
        return "Error missing 'key' parameter for /get.";
    }

    keyPos += 4;
    std::string key = urlDecode(query.substr(keyPos));

    {
        //std::lock_guard<std::mutex> lock(g_store_mutex);

        /*
        size_t keyPos = query.find("key=");
        if(keyPos == std::string::npos) 
        {
            http_status = "400 Bad Request";
            return "Error missing 'key' parameter for /get.";
        }

        keyPos += 4;
        std::string key = urlDecode(query.substr(keyPos));

        */

        std::lock_guard<std::mutex> lock(g_store_mutex);
        auto it = store.find(key);
        if(it != store.end())
        {
            g_cache_hits++;
            Node* foundNode = it->second;
            std::cout << "[INFO] Found Key "  << key << " in Cache." << std::endl;
            foundNode->moveToFront(head);

            value_copy = foundNode->value;
            found = true;
            //return foundNode->value;
        }
    }
    if(found)
    {
        heavy_computation();
        return value_copy;
    }
    
    else
    {
        std::cout << "[INFO] Cache MISS for GET. Checking Backend Database for Key: " << key << std::endl;
        std::string backend_status = "200 OK";
        std::string path_and_query = "/db_get?key=" + urlEncode(key);
        std::string value_from_db = sendToBackend(path_and_query, backend_status);

        if (backend_status.rfind("200 OK", 0) == 0) 
        {
            std::cout << "[INFO] Found key in Backend DB. Inserting into Cache." << std::endl;

            std::lock_guard<std::mutex> lock(g_store_mutex);
            if(store.find(key) != store.end()) 
            {
                return store[key]->value; 
            }

            if(count_of_pairs == N)
            {
                Node* node_to_evict = tail->prev;
                std::cout << "[INFO] Cache full. Evicting LRU key: " << node_to_evict->key << std::endl;
                writeToBackendDB(node_to_evict, http_status);
                detachNode(node_to_evict);
                store.erase(node_to_evict->key);
                delete node_to_evict;
                count_of_pairs--;
            }

            Node* newNode = new Node(key, value_from_db);
            newNode->dirty = false;
            attachToFront(newNode);
            store[key] = newNode;
            count_of_pairs++;

            return value_from_db;
        }
        else
        {
            http_status = backend_status;
            std::cout << "[LOG] Key " << key << " not found in Backend DB (" << backend_status << ")." << std::endl;
            return "Error: Key : " + key + " Not Found.";
        }
    }
}

std::string handle_delete(const std::string& query, KeyValueStore& store, std::string& http_status)
{
    g_total_access++;

    std::lock_guard<std::mutex> lock(g_store_mutex);

    size_t keyPos = query.find("key=");
    if(keyPos == std::string::npos) {
        http_status = "400 Bad Request";
        return "Error missing 'key' parameter for /delete.";
    }
    keyPos += 4;
    std::string key = urlDecode(query.substr(keyPos));

    auto it = store.find(key);
    if(it != store.end())
    {
        g_cache_hits++;
        Node* node_to_delete = it->second;
        detachNode(node_to_delete);
        store.erase(it);
        delete node_to_delete;
        count_of_pairs--;
        std::cout << "[LOG] Deleted Key " << key << " from in-Memory Cache." << std::endl;
    }

    std::cout << "[INFO] Deleting Key " << key << " from Backend DB." << std::endl;
    std::string backend_status = "200 OK";
    std::string path_and_query = "/db_delete?key=" + urlEncode(key);
    std::string backend_response = sendToBackend(path_and_query, backend_status);

    if (backend_status.rfind("200 OK", 0) != 0) 
    {
        http_status = backend_status;
        return "Error: Failed to delete key from Backend DB: " + backend_response;
    }

    return "Key: " + key + " deleted (from cache and DB)";
}

void flushAllToDB(KeyValueStore& store, std::string& http_status)
{
    std::lock_guard<std::mutex> lock(g_store_mutex);

    std::cout << "[INFO] Flushing all dirty nodes to Backend DB during shutdown..." << std::endl;
    Node* current = head->next;
    int count = 0;

    while (current != tail)
    {
        if (current->dirty)
        {
            writeToBackendDB(current, http_status);
            if (http_status.rfind("200 OK", 0) == 0) {
                count++;
                current->dirty = false;
            }
        }
        current = current->next;
    }
    std::cout << "[INFO] Flushed " << count << " dirty nodes to Backend." << std::endl;
}

void signal_handler(int signum) 
{
    std::cout << "\n[INFO] SIGINT (Ctrl+C) received. Initiating shutdown..." << std::endl;
    g_shutdown_flag = 1;


    if(g_server_fd != -1)
    {
        close(g_server_fd);
        g_server_fd = -1;
    }
}

void handle_client(int new_socket, KeyValueStore& store);

void worker_function(ThreadSafeQueue& queue, KeyValueStore& store) 
{
    std::thread::id thread_id = std::this_thread::get_id();
    std::cout << "[INFO] Worker Thread " << thread_id  <<" starting." << std::endl;;

    while(true) 
    {
        int new_socket = queue.pop();
        if(new_socket == -1) 
        {
            std::cout << "[INFO] Worker thread " << thread_id << " exiting." << std::endl;
            break;
        }
        std::cout << "[INFO] Worker thread " << thread_id << " handling a new client." << std::endl;
        handle_client(new_socket, store);
    }
}

void handle_client(int new_socket, KeyValueStore& store)
{

    add_socket(new_socket);
    char client_ip[INET_ADDRSTRLEN];
    struct sockaddr_in client_address;
    socklen_t client_addrlen = sizeof(client_address);

    getpeername(new_socket, (sockaddr*)&client_address, &client_addrlen);

    inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
    int client_port = ntohs(client_address.sin_port);

    std::cout << "[INFO] Thread " << std::this_thread::get_id() <<" handling client from " <<client_ip << ":" <<client_port<< std::endl;

    while (true)
    {
        char buffer[BUFFER_SIZE] = {0};
        int bytes_read = read(new_socket, buffer, BUFFER_SIZE);

        if (bytes_read <= 0) break;

        std::string request(buffer);
        std::string response_body;
        std::string http_status = "200 OK";
        bool keep_alive = true;

        size_t start = request.find("GET /") + 5;
        size_t end = request.find(" ", start);

        if (start == std::string::npos || end == std::string::npos)
        {
            http_status = "400 Bad Request";
            response_body = "Error: Malformed Request"; 
        }
        else
        {
            std::string pathAndQuery = request.substr(start, end - start);
            size_t queryPos = pathAndQuery.find("?");
            std::string path = pathAndQuery.substr(0, queryPos);
            std::string query = (queryPos != std::string::npos) ? pathAndQuery.substr(queryPos + 1) : "";

            if (path == "set")
                response_body += handle_set(query, store, http_status);
            else if (path == "get")
                response_body += handle_get(query, store, http_status);
            else if (path == "delete")
                response_body += handle_delete(query, store, http_status);
            else if (path == "disconnect") {
                response_body = "OK Disconnecting. ";
                keep_alive = false;
            }
            else 
            {
                http_status = "400 Bad Request";
                response_body = "Usage: /set, /get, /delete, /disconnect\n";
            }
        }

        std::string http_response = "HTTP/1.1 " + http_status + "\r\n";
        http_response += "Content-Type: text/plain\r\n";
        http_response += "Content-Length: " + std::to_string(response_body.length()) + "\r\n";
        http_response += (keep_alive) ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
        http_response += "\r\n";
        http_response += response_body;

        send(new_socket, http_response.c_str(), http_response.length(), 0);

        if (!keep_alive) break;
    }

    remove_socket(new_socket);
    close(new_socket);
    std::cout << "[INFO] Thread " << std::this_thread::get_id() << " finished. Closing Connection." << std::endl;
}

int main()
{
    head->next = tail;
    tail->prev = head;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    if (sigaction(SIGINT, &sa, NULL) == -1) 
    {
        perror("sigaction failed");
        return 1;
    }

    if (!connectToBackend()) 
    {
        std::cerr << "[FATAL] Failed to connect to Backend DB. Shutting down Frontend." << std::endl;
        return 1;
    }

    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(g_server_fd < 0) 
    { 
        perror("Socket creation failed"); 
        return 1; 
    }

    int opt = 1;
    if (setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) 
    {
        perror("setsockopt failed");
        close(g_server_fd);
        return 1;
    }

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(FRONTEND_PORT);

    if (bind(g_server_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) 
    {
        perror("Bind failed");
        close(g_server_fd);
        return 1;
    }

    if(listen(g_server_fd, 100) < 0) 
    {
        perror("Listen failed");
        close(g_server_fd);
        return 1;
    }

    std::cout << "Key-Value FRONTEND Server Listening on port " << FRONTEND_PORT << std::endl;
    std::cout << "Backend DB connected at " << BACKEND_IP << ":" << BACKEND_PORT << std::endl;

    KeyValueStore keyValueStore;
    ThreadSafeQueue task_queue;
    std::vector<std::thread> thread_pool;

    std::cout << "[INFO] Starting Thread Pool with " << NUM_THREADS << " threads." << std::endl;

    for(int i=0; i<NUM_THREADS; i++) 
    {
        thread_pool.emplace_back(worker_function, 
            std::ref(task_queue), 
            std::ref(keyValueStore)
        );
    }

    while(!g_shutdown_flag)
    {
        struct sockaddr_in client_address;
        socklen_t client_addrlen = sizeof(client_address);
        int new_socket = -1;

        new_socket = accept(g_server_fd, (struct sockaddr *)&client_address, &client_addrlen);

        if(new_socket < 0)
        {
            if(g_shutdown_flag || errno == EINTR) {
                std::cout << "[INFO] Accept Loop Interrupted." << std::endl;
                break;
            }
            perror("Connection Accept Failed");
            continue;
        }

        std::cout << "[INFO] Main Thread accepted new connection. Pushing to Queue. " << std::endl;
        task_queue.push(new_socket);
    }

    std::cout << "\n[INFO] Server shutting down." << std::endl;

    std::cout << "[INFO] Stopping task queue and notifying workers... " << std::endl;
    {
        std::lock_guard<std::mutex> lock(g_active_socket_list_mutex);
    
        std::string goodbye_msg = "HTTP/1.1 503 Service Unavailable\r\n"
                                  "Connection: close\r\n"
                                  "Content-Length: 25\r\n\r\n"
                                  "Server is shutting down.\n";

        for(int sock : g_active_sockets)
        {
            send(sock, goodbye_msg.c_str(), goodbye_msg.length(), 0);

            shutdown(sock, SHUT_RDWR);
            close(sock);
        }
    
    std::cout << "[INFO] Sent shutdown message to " << g_active_sockets.size() << " active clients." << std::endl;
    }
    
    task_queue.stop();

    for(std::thread& t: thread_pool) 
    {
        t.join();
    }
    std::cout << "[INFO] All worker threads have exited." << std::endl;

    std::string dummy;
    flushAllToDB(keyValueStore, dummy);

    Node* current = head->next;
    while(current != tail) 
    {
        Node* to_delete = current;
        current = current->next;
        delete to_delete;
    }
    delete head;
    delete tail;

    close(g_server_fd);
    
    if (g_backend_sock != -1) 
    {
        close(g_backend_sock);
    }


    std::cout << "\n========================================" << std::endl;
    std::cout << "          PERFORMANCE METRICS           " << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Total Read Requests: " << g_total_access << std::endl;
    std::cout << "Cache Hits:          " << g_cache_hits << std::endl;
    
    double hit_ratio = 0.0;
    if (g_total_access > 0) 
    {
        hit_ratio = (double)g_cache_hits / g_total_access * 100.0;
    }
    
    std::cout << "Cache Hit Ratio:     " << hit_ratio << "%" << std::endl;
    std::cout << "========================================" << std::endl;

    std::cout << "[INFO] Shutdown complete. " << std::endl;
    return 0;
}
