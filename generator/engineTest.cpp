#include <arpa/inet.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <getopt.h>
#include <iostream>
#include <list>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

#include "fmt/core.h"

const uint32_t distribution_seed = 1531445423;
std::list<std::string> adSource, checkoutSource;

struct CommandLineOptions
{
    size_t recordCount;
    int portAd;
    int portCheckout;
    long sleepTime;
    int dataGeneratedAfterEachSleep;
    int spikeInterval;
    int spikeMagnitude;
    size_t bufferThreshold;
    std::string adSourcePath;
    std::string checkoutSourcePath;
};

struct ProtectedBuffer
{
    explicit ProtectedBuffer(size_t threshold)
        : threshold(threshold)
        , currentlyInserted(0)
    {
        container.reserve(threshold);
    };

    std::vector<std::string> container;
    std::mutex mutex;
    std::condition_variable bufferEmptyCondition, bufferFullCondition;
    const size_t threshold;
    int currentlyInserted;
};

void sleepMilliseconds(unsigned long long ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

std::string generateAdByteStream()
{
    std::string s = adSource.front() + "\n";
    adSource.pop_front();
    return s;
}

std::string generateCheckoutByteStream()
{
    std::string s = checkoutSource.front() + "\n";
    checkoutSource.pop_front();
    return s;
}

void readFile(const std::string& filename, std::list<std::string>& lines)
{
    lines.clear();
    std::ifstream file(filename);
    std::string s;
    while (getline(file, s))
        lines.push_back(s);
}

int createServerSocket(int port)
{
    // Create server socket
    int socketDesc = socket(AF_INET, SOCK_STREAM, 0);
    if (socketDesc == -1) {
        std::cerr << "ERROR: Opening socket failed.\n";
        exit(2);
    }

    // Prevents "Address already used"-error on restart
    int option = 1;
    setsockopt(socketDesc, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));

    sockaddr_in server_addr, client_addr;

    // Setup the socket address structure to use in bind call
    server_addr.sin_family = AF_INET;

    // Will bind to any possible ip address
    server_addr.sin_addr.s_addr = INADDR_ANY;

    // Converts the port number into big-endian network byte order
    server_addr.sin_port = htons(port);
    std::cout << "Port: " << port << std::endl;

    // Bind the socket to an ip address and port number
    if (bind(socketDesc, (sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "ERROR: Binding socket to socket address [ip address + port number] failed.\n";
        std::cerr << strerror(errno) << "\n";
        exit(3);
    }

    // Listen for incoming connections
    if (listen(socketDesc, SOMAXCONN) == -1) {
        std::cerr << "ERROR: Listening for incoming connections failed.\n";
        exit(4);
    }

    // Accept incoming connection
    socklen_t client_len = sizeof(client_addr);
    int clientSocket = accept(socketDesc, (sockaddr*)&client_addr, &client_len);
    if (clientSocket == -1) {
        std::cerr << "ERROR: Accepting incoming connection failed. Client socket not created correctly.\n";
        exit(5);
    }

    return clientSocket;
}

void produce(CommandLineOptions options, ProtectedBuffer& buffer, std::function<std::string()> const& generator)
{
    bool isSpikeActive = false;

    for (size_t i = 0; i < options.recordCount;) {
        for (int k = 0; k < options.dataGeneratedAfterEachSleep && i < options.recordCount; k++, i++) {
            std::string generatedString = generator();
            {
                //std::unique_lock<std::mutex> lock(buffer.mutex);
                //buffer.bufferFullCondition.wait(lock, [&buffer] { return buffer.container.size() < buffer.threshold; });
                //buffer.container.push_back(std::move(generatedString));
                buffer.container[i] = std::move(generatedString);
                buffer.currentlyInserted++;
                buffer.bufferEmptyCondition.notify_one();
            }

            if (options.spikeInterval == 0 || options.spikeMagnitude == 0 || i % options.spikeInterval != 0)
                continue;

            if (isSpikeActive) {
                options.dataGeneratedAfterEachSleep = options.dataGeneratedAfterEachSleep / options.spikeMagnitude;
                std::cout << "Off " << options.dataGeneratedAfterEachSleep << "\n";
            } else {
                options.dataGeneratedAfterEachSleep = options.dataGeneratedAfterEachSleep * options.spikeMagnitude;
                std::cout << "On " << options.dataGeneratedAfterEachSleep << "\n";
            }
            isSpikeActive = !isSpikeActive;
        }

        if (options.sleepTime != 0) {
            sleepMilliseconds(options.sleepTime);
        }
    }
}

void consume(int socket, CommandLineOptions options, ProtectedBuffer& buffer)
{
    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Starting consumer\n";

    uint64_t totalBytes = 0;
    std::string element;
    element.reserve(10000);

    for (int i = 0; i < options.recordCount;) {
        element = "";
        {
            std::unique_lock<std::mutex> lock(buffer.mutex);
            //buffer.bufferEmptyCondition.wait(lock, [&buffer] { return !buffer.container.empty(); });
            buffer.bufferEmptyCondition.wait(lock, [&buffer, &i] { return i < buffer.currentlyInserted; });
            //buffer.bufferFullCondition.notify_all();
        }
        for (int j = 0; j < 10 && i < buffer.currentlyInserted; j++) {
            //fmt::format_to(std::back_inserter(element), FMT_COMPILE("{}{}\n"), buffer.container[i], currentTimestamp);
            element += buffer.container[i];
            i++;
            if (i % 1000000 == 0)
                std::cout << "Thread #" << std::this_thread::get_id() << ": " << i << " Tuples sent.\n";
        }

        totalBytes += element.length();

        int res = write(socket, element.c_str(), element.length());
        if (res == -1) {
            std::cerr << "Thread #" << std::this_thread::get_id() << ": "
                      << "Error. Sending bytes via socket stream failed: " << strerror(errno) << "\n";
            close(socket);
            exit(EXIT_FAILURE);
        }
    }

    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Sent " << totalBytes << " Bytes\n";
    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Producer done\n";
}

void print_usage()
{
    printf("Usage: generator --count num [--logInterval num] --portAd num --portCheckout num --sleepTime num --dataGeneratedAfterEachSleep "
           "--adSourcePath str --checkoutSourcePath str"
           "num [--sustainabilityLimit num] [--spikeInterval num] [--spikeMagnitude num] [--bufferThreshold num]\n");
}

CommandLineOptions parse_arguments(int argc, char* argv[])
{
    static struct option long_options[] = {
        {"count", required_argument, 0, 'c'},
        {"portAd", required_argument, 0, 'p'},
        {"portCheckout", required_argument, 0, 'q'},
        {"sleepTime", required_argument, 0, 's'},
        {"dataGeneratedAfterEachSleep", required_argument, 0, 'g'},
        {"spikeInterval", required_argument, 0, 'i'},
        {"spikeMagnitude", required_argument, 0, 'm'},
        {"bufferThreshold", required_argument, 0, 't'},
        {"adSourcePath", required_argument, 0, 'a'},
        {"checkoutSourcePath", required_argument, 0, 'b'},
        {0, 0, 0, 0}};

    CommandLineOptions options{
        .dataGeneratedAfterEachSleep = -1,
        .spikeInterval = 0,
        .spikeMagnitude = 0,
        .bufferThreshold = 100,
    };

    int long_index = 0, opt;
    while ((opt = getopt_long(argc, argv, "apl:b:", long_options, &long_index)) != -1) {
        switch (opt) {
        case 'c':
            options.recordCount = std::stoull(optarg);
            break;
        case 'p':
            options.portAd = atoi(optarg);
            break;
        case 'q':
            options.portCheckout = atoi(optarg);
            break;
        case 's':
            options.sleepTime = atoi(optarg);
            break;
        case 'g':
            options.dataGeneratedAfterEachSleep = atoi(optarg);
            break;
        case 'i':
            options.spikeInterval = atoi(optarg);
            break;
        case 'm':
            options.spikeMagnitude = atoi(optarg);
            break;
        case 't':
            options.bufferThreshold = std::stoull(optarg);
            break;
        case 'a':
            options.adSourcePath = optarg;
            break;
        case 'b':
            options.checkoutSourcePath = optarg;
            break;
        default:
            print_usage();
            exit(EXIT_FAILURE);
        }
    }

    if (options.recordCount == -1 || options.portAd == -1 || options.portCheckout == -1 || options.sleepTime == -1
        || options.dataGeneratedAfterEachSleep == -1) {
        print_usage();
        exit(EXIT_FAILURE);
    }

    return options;
}

int main(int argc, char* argv[])
{
    auto options = parse_arguments(argc, argv);

    auto readAdsFuture = std::async(std::launch::async, [&] { return readFile(options.adSourcePath, adSource); });
    auto readCheckoutsFuture = std::async(std::launch::async, [&] { return readFile(options.checkoutSourcePath, checkoutSource); });
    readAdsFuture.get();
    readCheckoutsFuture.get();

    ProtectedBuffer bufferAd{options.recordCount}, bufferCheckout{options.recordCount};

    auto socketAdTaskFuture = std::async(std::launch::async, [&] { return createServerSocket(options.portAd); });
    auto socketCheckoutTaskFuture = std::async(std::launch::async, [&] { return createServerSocket(options.portCheckout); });

    int socketAd = socketAdTaskFuture.get();
    int socketCheckout = socketCheckoutTaskFuture.get();

    std::vector<std::thread> producers;
    for (int i = 0; i < 1; i++) {
        producers.emplace_back(produce, options, std::ref(bufferAd), std::ref(generateAdByteStream));
        producers.emplace_back(produce, options, std::ref(bufferCheckout), std::ref(generateCheckoutByteStream));
    }

    std::thread consumerAd(consume, socketAd, options, std::ref(bufferAd));
    std::thread consumerCheckout(consume, socketCheckout, options, std::ref(bufferCheckout));

    for (auto& producer : producers) {
        producer.join();
    }

    consumerAd.join();
    consumerCheckout.join();
    close(socketAd);
    close(socketCheckout);
    return 0;
}
