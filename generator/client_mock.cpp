/*
 * This program measures the throughput of the incoming socket data stream on the specified port.
 * Used to benchmark the data generator in an isolated context.
 * Usage: client_mock <port-number>
 *
 * Run the data generator that writes to the socket (the same port number). Only one port can be measured within one execution.
 * When the data generator closes the TCP connection, the client_mock will print the measured results.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <chrono>
#include <cerrno>
#include <iomanip>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

int bufferSize = 4096 * 8 * 8 * 8;

int main(int argc, char* argv[])
{
    if (argc < 2) {
        std::cerr << "Usage: ./server_mock <port_number>\n";
        exit(1);
    }

    // Get port number as command line argument
    int port = atoi(argv[1]);

    sockaddr_in server_addr{};
    memset(&server_addr, 0, sizeof(sockaddr_in));

    int socketDesc = socket(AF_INET, SOCK_STREAM, 0);
    if (socketDesc == -1) {
        std::cerr << "Error. Could not create socket: " << strerror(errno) << "\n";
        exit(EXIT_FAILURE);
    }

    // Increase TCP receive buffer size
    if (setsockopt(socketDesc, SOL_SOCKET, SO_RCVBUF, &bufferSize, sizeof(bufferSize)) == -1) {
        std::cerr << "ERROR: Setting send buffer size failed.\n";
    }

    // Prevents "Address already used"-error on restart
    int option = 1;
    setsockopt(socketDesc, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    int res = connect(socketDesc, (sockaddr*)&server_addr, sizeof(server_addr));
    if (res == -1) {
        std::cerr << "Error. Could not connect to server socket: " << strerror(errno) << "\n";
        exit(EXIT_FAILURE);
    }

    std::cout << "Connected to TCP socket port " << port << ".\n";

    char buffer[bufferSize];

    size_t total_bytes_read = 0;
    bool initial_bytes_received = false;

    std::chrono::time_point<std::chrono::steady_clock> t1;
    std::chrono::time_point<std::chrono::steady_clock> t2;

    std::string surplus;

    while (true) {
        strncpy(buffer, surplus.c_str(), surplus.size());

        int bytesRead = recv(socketDesc, buffer + surplus.size(), bufferSize - surplus.size(), 0);

        if (bytesRead == -1) {
            std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
            exit(6);
        } else if (bytesRead + surplus.size() == 0) {
            t2 = std::chrono::steady_clock::now();
            std::cout << "SUCCESS: Socket Stream closed. Peer performed an orderly shutdown.\n";
            break;
        }

        if (!initial_bytes_received) {
            t1 = std::chrono::steady_clock::now();
            initial_bytes_received = true;
        }

        total_bytes_read += bytesRead;

        int nextStart = 0;
        for (int i = 0; i < bytesRead; i++) {
            if (buffer[i] == '\n') {
                nextStart = i + 1;
            }
        }

        surplus.assign(buffer + nextStart, bytesRead - nextStart);
    }

    std::cout << std::fixed << std::setprecision(9);
    double sec = std::chrono::duration<double>(t2 - t1).count();
    std::cout << "Time Duration: " << sec << "s\n";
    std::cout << "Total Bytes Read: " << total_bytes_read << "B\n";
    std::cout << "Total Throughput: " << (total_bytes_read / 1000000.0) / sec << " MB/s\n";

    close(socketDesc);

    return 0;
}
