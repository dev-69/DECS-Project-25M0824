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

#include <libpq-fe.h>

#include <queue>
#include <condition_variable>

#define BACKEND_PORT 7000
#define BUFFER_SIZE 2048
const int NUM_THREADS = 8; 

const std::string db_NAME = "KEY_VALUE";
const std::string table_NAME = "KV_Store";

volatile sig_atomic_t g_shutdown_flag = 0;

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
            
            if(m_stop && m_queue.empty())
            {
                return -1;
            }
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
        }
        else
        {
            decoded += str[i];
        }
    }
    return decoded;
}

void create_Key_Value_Table(PGconn* conn)
{
    std::string sql_command = "CREATE TABLE IF NOT EXISTS " + table_NAME +
                              "("
                              "    key   TEXT PRIMARY KEY,"
                              "    value TEXT"
                              ")";

    PGresult* res = PQexec(conn, sql_command.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        std::cerr << "SQL command failed (CREATE TABLE): " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);
}

std::string handle_db_set(const std::string& query, std::string& http_status, PGconn* conn)
{
    size_t keyPos = query.find("key=");
    size_t valPos = query.find("value=");

    if(keyPos == std::string::npos || valPos == std::string::npos)
    {
        http_status = "400 Bad Request";
        return "Error missing 'key' or 'value' parameter for /db_set.";
    }

    keyPos += 4;
    valPos += 6;

    std::string key = urlDecode(query.substr(keyPos, query.find("&", keyPos) - keyPos));
    std::string value = urlDecode(query.substr(valPos));

    std::string sql_command =
        "INSERT INTO " + table_NAME + " (key, value) VALUES ($1, $2) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value";

    const char *paramValues[2] = {key.c_str(), value.c_str()};
    const Oid paramTypes[2] = {0, 0}; 
    
    PGresult *res = PQexecParams(
        conn,
        sql_command.c_str(),
        2,
        paramTypes,
        paramValues,
        NULL, 
        NULL, 
        0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        std::cerr << "SQL command failed (SET): " << PQerrorMessage(conn) << std::endl;
        http_status = "500 Internal Server Error";
        PQclear(res);
        return "ERROR: Database write failed.";
    }

    std::cout << "[DB LOG] SET Key " << key << " successful." << std::endl;
    PQclear(res);
    return "OK";
}

std::string handle_db_get(const std::string& query, std::string& http_status, PGconn* conn)
{
    size_t keyPos = query.find("key=");
    if(keyPos == std::string::npos)
    {
        http_status = "400 Bad Request";
        return "Error missing 'key' parameter for /db_get.";
    }

    keyPos += 4;
    std::string key = urlDecode(query.substr(keyPos));

    std::string sql_command =
        "SELECT value FROM " + table_NAME + " WHERE key = $1";
    const char *paramValues[1] = {key.c_str()};
    const Oid paramTypes[1] = {0};

    PGresult *res = PQexecParams
    (
        conn,
        sql_command.c_str(),
        1,
        paramTypes,
        paramValues,
        NULL,
        NULL,
        0
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        std::cerr << "SQL command failed (GET): " << PQerrorMessage(conn) << std::endl;
        http_status = "500 Internal Server Error";
        PQclear(res);
        return "ERROR: Database read failed.";
    }
    
    if(PQntuples(res) > 0)
    {
        std::string value_from_db(PQgetvalue(res, 0, 0));
        std::cout << "[DB LOG] GET Key " << key << " found." << std::endl;
        PQclear(res);
        return value_from_db;
    }
    else
    {
        PQclear(res);
        http_status = "404 Not Found";
        std::cout << "[DB LOG] GET Key " << key << " not found." << std::endl;
        return "Error: Key Not Found.";
    }
}

std::string handle_db_delete(const std::string& query, std::string& http_status, PGconn* conn)
{
    size_t keyPos = query.find("key=");
    if(keyPos == std::string::npos)
    {
        http_status = "400 Bad Request";
        return "Error missing 'key' parameter for /db_delete.";
    }
    keyPos += 4;    
    std::string key = urlDecode(query.substr(keyPos));
    
    std::string sql_command = 
        "DELETE FROM " + table_NAME + " WHERE key = $1";
    const char* paramValues[1] = {key.c_str()};
    const Oid paramTypes[1] = {0};

    PGresult* res = PQexecParams
    (
        conn,
        sql_command.c_str(),
        1,
        paramTypes,
        paramValues,
        NULL,
        NULL,
        0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        std::cerr << "SQL command failed (DELETE): " << PQerrorMessage(conn) << std::endl;
        http_status = "500 Internal Server Error";
        PQclear(res);
        return "ERROR: Database delete failed.";
    }
    
    long rows_deleted = 0;
    if (PQcmdTuples(res) != NULL)
        rows_deleted = std::atol(PQcmdTuples(res));

    PQclear(res);

    if(rows_deleted > 0)
    {
        std::cout << "[DB LOG] DELETE Key " << key << " successful (" << rows_deleted << " rows affected)." << std::endl;
        return "OK";
    }
    else
    {
        std::cout << "[DB LOG] DELETE Key " << key << " not found in DB." << std::endl;
        http_status = "404 Not Found";
        return "Error: Key Not Found in Database.";
    }
}

void signal_handler(int signum)
{
    std::cout << "\n[INFO] SIGINT (Ctrl+C) received. Initiating shutdown..." << std::endl;
    g_shutdown_flag = 1;
}

void handle_client(int new_socket, PGconn* conn);

void worker_function(ThreadSafeQueue& queue, PGconn* conn)
{
    std::thread::id thread_id = std::this_thread::get_id();
    std::cout << "[INFO] Worker Thread " << thread_id  <<" starting." << std::endl;;

    const std::string conninfo = "dbname=" + db_NAME + " user=dev password='123456' hostaddr=127.0.0.1 port=5432";
    PGconn* worker_conn = PQconnectdb(conninfo.c_str());

    if(PQstatus(worker_conn) != CONNECTION_OK)
    {
        std::cerr << "[FATAL] Worker " << thread_id << " failed to connect to DB: " << PQerrorMessage(worker_conn) << std::endl;
        PQfinish(worker_conn);
        return;
    }
    std::cout << "[INFO] Worker " << thread_id << " established its own DB connection." << std::endl;


    while(true)
    {
        int new_socket = queue.pop();

        if(new_socket == -1)
        {
            std::cout << "[INFO] Worker thread " << thread_id << " exiting." << std::endl;
            break;
        }

        std::cout << "[INFO] Worker thread " << thread_id << " handling a new Frontend connection." << std::endl;
        handle_client(new_socket, worker_conn);
    }

    PQfinish(worker_conn);
}

void handle_client(int new_socket, PGconn* conn)
{
    while (true)
    {
        char buffer[BUFFER_SIZE] = {0};
        int bytes_read = read(new_socket, buffer, BUFFER_SIZE);

        if (bytes_read <= 0) break;

        std::string request(buffer);
        std::string response_body;
        std::string http_status = "200 OK";
        bool keep_alive = true;

        if (request.find("Connection: close") != std::string::npos) 
        {
            keep_alive = false;
        }
        
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

            if (path == "db_set")
                response_body = handle_db_set(query, http_status, conn);
            else if (path == "db_get")
                response_body = handle_db_get(query, http_status, conn);
            else if (path == "db_delete")
                response_body = handle_db_delete(query, http_status, conn);
            else
            {
                http_status = "404 Not Found";
                response_body = "Internal API: /db_set, /db_get, /db_delete\n";
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

    close(new_socket);
    std::cout << "[INFO] Worker finished with Frontend connection. Closing socket." << std::endl;
}

int main()
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));      
    sa.sa_handler = signal_handler;  
    if (sigaction(SIGINT, &sa, NULL) == -1) 
    {
        perror("sigaction failed");
        return 1;
    }
    
    const std::string conninfo = "dbname=" + db_NAME + " user=dev password='123456' hostaddr=127.0.0.1 port=5432";
    PGconn* main_conn = PQconnectdb(conninfo.c_str());

    if(PQstatus(main_conn) != CONNECTION_OK)
    {
        std::cerr << "Connection to database failed: " << PQerrorMessage(main_conn) << std::endl;
        PQfinish(main_conn);
        return 1; 
    }
    std::cout << "Successfully connected to PostgreSQL database." << std::endl;
    create_Key_Value_Table(main_conn);
    PQfinish(main_conn);


    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(server_fd < 0) 
    { 
        perror("Socket creation failed"); 
        return 1; 
    }


    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) 
    {
        perror("setsockopt failed");
        close(server_fd);
        return 1;
    }

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(BACKEND_PORT); 

    if (bind(server_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) 
    {
        perror("Bind failed");
        close(server_fd);
        return 1;
    }

    if(listen(server_fd, 100) < 0) 
    {
        perror("Listen failed");
        close(server_fd);
        return 1;
    }

    std::cout << "Key-Value BACKEND Server Listening for Frontend connections on port " << BACKEND_PORT << std::endl;

    ThreadSafeQueue task_queue;
    std::vector<std::thread> thread_pool;

    std::cout << "[INFO] Starting Thread Pool with " << NUM_THREADS << " threads." << std::endl;

    for(int i=0; i<NUM_THREADS; i++)
    {
        thread_pool.emplace_back(worker_function, std::ref(task_queue), nullptr);
    }

    while(!g_shutdown_flag)
    {
        struct sockaddr_in client_address;
        socklen_t client_addrlen = sizeof(client_address);
        int new_socket = -1; 

        new_socket = accept(server_fd, (struct sockaddr *)&client_address, &client_addrlen);

        if(new_socket < 0)
        {
            if(g_shutdown_flag || errno == EINTR) {
                std::cout << "[INFO] Accept Loop Interrupted." << std::endl;
                break;
            }
            perror("Connection Accept Failed");
            continue;
        }

        std::cout << "[INFO] Main Thread accepted new Frontend connection. Pushing to Queue. " << std::endl;
        task_queue.push(new_socket);
    }

    std::cout << "\n[INFO] Server shutting down." << std::endl;
    task_queue.stop();

    for(std::thread& t: thread_pool) {
        t.join();
    }
    std::cout << "[INFO] All worker threads have exited." << std::endl;
    
    close(server_fd);

    std::cout << "[INFO] Shutdown complete. " << std::endl;
    return 0;
}