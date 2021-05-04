#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#include "Definitions.h"
#include "GeneratingOperator.h"

const size_t bufferSize = 4096 * 8 * 8 * 8;

class _TCPReceiveOperator : public _GeneratingOperator<std::string>
{
public:
    explicit _TCPReceiveOperator(int port) { connectToServerSocket(port); }

    std::string execute() override
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (stringBuffer.empty()) {
            readDataFromSocket();
        }

        auto next = stringBuffer.front();
        stringBuffer.pop_front();
        return next;
    }

    ~_TCPReceiveOperator() { close(socketDescriptor); }

private:
    void readDataFromSocket()
    {
        char buffer[bufferSize];

        int bytesRead = recv(socketDescriptor, buffer, bufferSize, 0);

        std::string string = surplus;
        string += std::string(buffer, bytesRead);
        bytesRead += surplus.length();

        if (bytesRead == -1) {
            std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
            exit(6);
        } else if (bytesRead == 0) {
            throw std::runtime_error("End of pipeline");
        }

        std::string::size_type pos;
        std::string::size_type prev = 0;
        while ((pos = string.find('\n', prev)) != std::string::npos) {
            std::string line = string.substr(prev, pos - prev);
            stringBuffer.push_back(line);
            prev = pos + 1;
        }

        if (prev < string.length()) {
            surplus = string.substr(prev);
        } else {
            surplus = "";
        }
    }

    void connectToServerSocket(int port)
    {
        sockaddr_in serverAddr{};
        memset(&serverAddr, 0, sizeof(sockaddr_in));

        socketDescriptor = socket(AF_INET, SOCK_STREAM, 0);
        if (socketDescriptor == -1) {
            std::cerr << "Error. Could not create socket: " << strerror(errno) << "\n";
            exit(EXIT_FAILURE);
        }

        // Increase TCP receive buffer size
        if (setsockopt(socketDescriptor, SOL_SOCKET, SO_RCVBUF, &bufferSize, sizeof(bufferSize)) == -1) {
            std::cerr << "ERROR: Setting send buffer size failed.\n";
        }

        // Prevents "Address already used"-error on restart
        int option = 1;
        setsockopt(socketDescriptor, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));

        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

        int success = connect(socketDescriptor, (sockaddr*)&serverAddr, sizeof(serverAddr));
        if (success == -1) {
            std::cerr << "Error. Could not connect to server socket: " << strerror(errno) << "\n";
            exit(EXIT_FAILURE);
        }
    }

    int socketDescriptor;
    std::deque<std::string> stringBuffer;
    std::string surplus;
    std::mutex mutex;
};
DECLARE_SHARED_PTR_NON_TEMPLATE(TCPReceiveOperator);
