#include <iostream>
#include <string>
#include <vector>
#include <sstream>

#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#define BUFFER_SIZE 2048

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

std::string getResponseBody(const std::string& response)
{
    size_t body_start = response.find("\r\n\r\n");
    if(body_start != std::string::npos)
    {
        return response.substr(body_start+4);
    }
    return "Error: Malformed Response from Server.";
}

void handle_connect(int &sock_fd, const char* server_ip, int server_port)
{
    if(sock_fd != -1)
    {
        std::cout << "Already connected. Use Disconnect first." << std::endl;
        return;
    }

    //1. Creating socket
    sock_fd = socket(AF_INET, SOCK_STREAM ,0);
    if(sock_fd < 0)
    {
        perror("Socket creation error");
        return;
    }

    //2. Defining server address
    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(server_port);
    if(inet_pton(AF_INET, server_ip, &server_address.sin_addr) <= 0)
    {
        perror("Invalid address/ Address not supoorted");
        close(sock_fd);
        sock_fd = -1;
        return;
    }

    //3. Connecting to server
    if(connect(sock_fd, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) 
    {
        perror("Connection Failed");
        close(sock_fd);
        sock_fd = -1;
        return;
    }

    std::cout << "Successfully connected to the Server. " << std::endl;
}


void handle_disonnect(int &sock_fd)
{
    std::string http_request = "GET /disconnect HTTP/1.1\r\nConnection: close\r\n\r\n";
    send(sock_fd, http_request.c_str(), http_request.length(), 0);

    char buffer[BUFFER_SIZE] = {0};
    read(sock_fd, buffer, BUFFER_SIZE);
    std::cout << "Server: " << getResponseBody(buffer) << std::endl;

    close(sock_fd);
    sock_fd = -1;
    std::cout << "Disconnected from the server " << std::endl;
}

void handle_create_update(int sock_fd, std::stringstream& ss, const std::string& command)
{
    if(sock_fd == -1)
    {
        std::cout << "Not connected." << std::endl;
        return;
    }

    std::string key, value;
    ss >> key;
    std::getline(ss >> std::ws, value);

    if(key.empty() || value.empty())
    {
        std::cout << "Usage: " << command << " <key> <value> " << std::endl;
        return;
    }

    std::string path = "/set?key=" + urlEncode(key) + "&value=" + urlEncode(value);
    std::string http_request = "GET " + path + " HTTP/1.1\r\nConnection: keep-alive\r\n\r\n";

    send(sock_fd, http_request.c_str(), http_request.length(), 0);

    char buffer[BUFFER_SIZE] = {0};
    read(sock_fd, buffer, BUFFER_SIZE);
    std::cout << "Server: " << getResponseBody(buffer) << std::endl;
}

void handle_read(int sock_fd, std::stringstream& ss)
{
    if(sock_fd == -1)
    {
        std::cout << "Not connected." << std::endl;
        return;
    }

    std::string key;
    ss >> key;
    if(key.empty())
    {
        std::cout << "Usage: READ <key> " << std::endl;
        return;
    }

    std::string path = "/get?key=" + urlEncode(key);
    std:: string http_request = "GET " + path + " HTTP/1.1\r\nConnection: keep-alive\r\n\r\n";

    send(sock_fd, http_request.c_str(), http_request.length(), 0);

    char buffer[BUFFER_SIZE] = {0};
    read(sock_fd, buffer, BUFFER_SIZE);
    std::cout << "Server: " << getResponseBody(buffer) << std::endl;
}

void handle_delete(int sock_fd, std::stringstream& ss)
{
    if(sock_fd == -1)
    {
        std::cout << "Not connected." << std::endl;
        return;
    }

    std::string key;
    ss >> key;

    if(key.empty())
    {
        std::cout << "Usage: DELETE <key> " << std::endl;
        return;
    }

    std::string path = "/delete?key=" + urlEncode(key);
    std:: string http_request = "GET " + path + " HTTP/1.1\r\nConnection: keep-alive\r\n\r\n";

    send(sock_fd, http_request.c_str(), http_request.length(), 0);

    char buffer[BUFFER_SIZE] = {0};
    read(sock_fd, buffer, BUFFER_SIZE);
    std::cout << "Server: " << getResponseBody(buffer) << std::endl;
}

void handle_help() 
{
    std::cout << "Available commands:\n"
              << "  CONNECT                      - Connect to the server\n"
              << "  CREATE <key> <value>         - Create a new key-value pair\n"
              << "  READ <key>                   - Read the value of a key\n"
              << "  UPDATE <key> <value>         - Update the value of a key\n"
              << "  DELETE <key>                 - Delete a key-value pair\n"
              << "  DISCONNECT                   - Disconnect from the server\n"
              << "  EXIT                         - Exit the client" << std::endl;
}




int main(int argc, char * argv[])
{
    if(argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << "<server-ip-address> <server-port> " << std::endl;
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    int sock_fd = -1; // -1 indicates not connnected to server.

    std::cout << "Key-Value Client started. Type 'CONNECT' to begin." << std::endl;
    std::cout << "Type 'HELP' for a list of commands." << std::endl;

    while(true)
    {
        std::cout << "> ";
        std::string line;
        if(!std::getline(std::cin, line))
        {
            break;
        }

        std::stringstream ss(line);
        std::string command;
        ss >> command;


        if(command == "CONNECT")
        {
            handle_connect(sock_fd, server_ip, server_port);
        }
        else if(command == "DISCONNECT")
        {
            handle_disonnect(sock_fd);
        }
        else if(command == "CREATE" || command == "UPDATE")
        {
            handle_create_update(sock_fd, ss, command);
        }   
        else if(command == "READ")
        {
            handle_read(sock_fd, ss);
        }
        else if(command == "DELETE")
        {
            handle_delete(sock_fd, ss);
        }
        else if(command == "HELP")
        {  
            handle_help();
        }
        else if(command == "EXIT")
        {
            if(sock_fd != -1)
            {
                handle_disonnect(sock_fd);
            }
            break;
        }
        else
        {
            if(!command.empty())
            {
                std::cout << "Unknown Command: " << command << " type HELP for a list of commands" << std::endl;
            }
        }

    }

    return 0;
}

