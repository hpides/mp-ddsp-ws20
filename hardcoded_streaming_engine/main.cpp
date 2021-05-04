#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

const size_t bufferSize = 4096 * 8 * 8 * 8;

inline uint64_t getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

struct AdTuple
{
    int adId;
    int userId;
    double cost;
    uint64_t eventTime;
    uint64_t processingTime;
    uint64_t numTuple;
};

struct CheckoutTuple
{
    int purchaseId;
    int userId;
    int adId;
    double value;
    uint64_t eventTime;
    uint64_t processingTime;
};

template<typename T>
inline T parse(char*& pos)
{
    T ret = 0;
    while (*pos != ',')
        ret = ret * 10 + (*pos++ - '0');
    pos++;
    return ret;
}

template<typename T>
inline T parse(char*& pos, char* end)
{
    T ret = 0;
    while (pos != end)
        ret = ret * 10 + (*pos++ - '0');
    pos++;
    return ret;
}

template<>
inline double parse<double>(char*& pos)
{
    double ret = 0;
    while (*pos >= '0' && *pos <= '9')
        ret = ret * 10 + (*pos++ - '0');
    pos++;
    uint8_t numDecimals = 0;
    double tmp = 0.0;
    while (*pos != ',') {
        tmp = tmp * 10 + (*pos++ - '0');
        numDecimals++;
    }
    pos++;

    ret += tmp / std::pow(10.0, numDecimals);
    return ret;
}

int connectToServerSocket(int port)
{
    sockaddr_in serverAddr{};
    memset(&serverAddr, 0, sizeof(sockaddr_in));

    int socketDescriptor = socket(AF_INET, SOCK_STREAM, 0);
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

    return socketDescriptor;
}

int main()
{
    auto adInputSocketFuture = std::async(std::launch::async, [] { return connectToServerSocket(12345); });
    auto checkoutInputSocketFuture = std::async(std::launch::async, [] { return connectToServerSocket(12346); });

    auto adInputSocket = adInputSocketFuture.get();
    auto checkoutInputSocket = checkoutInputSocketFuture.get();

    std::chrono::milliseconds windowSizeMillis(5000);
    std::chrono::milliseconds windowSlideSizeMillis(1000);
    auto numWindowSlides = static_cast<std::deque<int>::size_type>(windowSizeMillis / windowSlideSizeMillis);
    std::deque<std::unordered_map<int, AdTuple>> adSlideContainers(1);
    std::deque<std::unordered_map<int, CheckoutTuple>> checkoutSlideContainers(1);
    std::mutex adMutex;
    std::mutex checkoutMutex;
    std::chrono::time_point<std::chrono::high_resolution_clock> currentSlideStartedTime = std::chrono::high_resolution_clock::now();
    /*bool isAdError = false, isCheckoutError = false;
    int emptyAdWindows = 0, emptyCheckoutWindows = 0;*/
    std::ofstream output("output.csv", std::ios::out | std::ios::trunc);

    // In a separate thread, collect elements from the pipeline into the new slide
    std::thread adThread([&] {
        char buffer[bufferSize];
        int socketDescriptor = adInputSocket;
        std::string surplus;
        while (true) {
            strncpy(buffer, surplus.c_str(), surplus.size());

            int bytesRead = recv(socketDescriptor, buffer + surplus.size(), bufferSize - surplus.size(), 0);
            bytesRead += surplus.size();

            if (bytesRead == -1) {
                std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
                exit(6);
            } else if (bytesRead == 0) {
                throw std::runtime_error("end of pipeline");
                /*isAdError = true;
                return;*/
            }


            int nextStart = 0;
            for (int i = 0; i < bytesRead; i++) {
                if (buffer[i] == '\n') {
                    int tupleLength = i - nextStart;
                    char* pos = buffer + nextStart;
                    char* end = pos + tupleLength;

                    AdTuple adTuple{
                        .adId = parse<int>(pos),
                        .userId = parse<int>(pos),
                        .cost = parse<double>(pos),
                        .eventTime = parse<uint64_t>(pos, end),
                        .processingTime = getCurrentUnixTimestampInMilliseconds(),
                        .numTuple = 1};

                    {
                        std::lock_guard lock(adMutex);
                        auto& adAggregateMap = adSlideContainers.back();
                        if (auto it = adAggregateMap.find(adTuple.adId); it == adAggregateMap.end()) {
                            adAggregateMap.emplace(adTuple.adId, adTuple);
                        } else {
                            auto& result = it->second;
                            result.cost += adTuple.cost;
                            result.eventTime = std::max(result.eventTime, adTuple.eventTime);
                            result.processingTime = std::max(result.processingTime, adTuple.processingTime);
                            result.numTuple += adTuple.numTuple;
                        }
                    }

                    nextStart = i + 1;
                }
            }

            surplus.assign(buffer + nextStart, bytesRead - nextStart);
        }
    });

    std::thread checkoutThread([&] {
        char buffer[bufferSize];
        int socketDescriptor = checkoutInputSocket;
        std::string surplus;
        while (true) {
            strncpy(buffer, surplus.c_str(), surplus.size());

            int bytesRead = recv(socketDescriptor, buffer + surplus.size(), bufferSize - surplus.size(), 0);
            bytesRead += surplus.size();

            if (bytesRead == -1) {
                std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
                exit(6);
            } else if (bytesRead == 0) {
                throw std::runtime_error("end of pipeline");
                /*isCheckoutError = true;
                return;*/
            }


            int nextStart = 0;
            for (int i = 0; i < bytesRead; i++) {
                if (buffer[i] == '\n') {
                    int tupleLength = i - nextStart;
                    char* pos = buffer + nextStart;
                    char* end = pos + tupleLength;

                    CheckoutTuple checkoutTuple{
                        .purchaseId = parse<int>(pos),
                        .userId = parse<int>(pos),
                        .adId = parse<int>(pos),
                        .value = parse<double>(pos),
                        .eventTime = parse<uint64_t>(pos, end),
                        .processingTime = getCurrentUnixTimestampInMilliseconds()};

                    nextStart = i + 1;
                    if (checkoutTuple.adId == 0) {
                        continue;
                    }
                    {
                        std::lock_guard lock(checkoutMutex);
                        auto& checkoutAggregateMap = checkoutSlideContainers.back();
                        if (auto it = checkoutAggregateMap.find(checkoutTuple.adId); it == checkoutAggregateMap.end()) {
                            checkoutAggregateMap[checkoutTuple.adId] = checkoutTuple;
                        } else {
                            auto& result = it->second;
                            result.value += checkoutTuple.value;
                            result.eventTime = std::max(result.eventTime, checkoutTuple.eventTime);
                            result.processingTime = std::max(result.processingTime, checkoutTuple.processingTime);
                        }
                    }
                }
            }

            surplus.assign(buffer + nextStart, bytesRead - nextStart);
        }
    });


    while (true) {
        // In main thread, wait until current slide is over
        currentSlideStartedTime += windowSlideSizeMillis;
        std::this_thread::sleep_until(std::chrono::time_point(currentSlideStartedTime));

        // AdStream
        {
            std::lock_guard lock(adMutex);
            adSlideContainers.emplace_back();

            /*if (isAdError)
                emptyAdWindows++;*/
        }

        // Aggregate part combined with collection from Slides
        std::unordered_map<int, AdTuple> adAggregateMap;
        for (int i = 0; i < std::min(numWindowSlides, adSlideContainers.size()); i++) {
            for (auto& [key, adTuple] : adSlideContainers[i]) {
                if (auto it = adAggregateMap.find(key); it == adAggregateMap.end()) {
                    adAggregateMap.emplace(key, adTuple);
                } else {
                    auto& result = it->second;
                    result.cost += adTuple.cost;
                    result.eventTime = std::max(result.eventTime, adTuple.eventTime);
                    result.processingTime = std::max(result.processingTime, adTuple.processingTime);
                    result.numTuple += adTuple.numTuple;
                }
            }
        }

        // Remove old slide (if not belonging to next window anymore)
        {
            std::lock_guard lock(adMutex);
            while (adSlideContainers.size() > numWindowSlides) {
                adSlideContainers.pop_front();
            }
        }

        // CheckoutStream
        {
            std::lock_guard lock(checkoutMutex);
            checkoutSlideContainers.emplace_back();

            /*if (isCheckoutError)
                emptyCheckoutWindows++;*/
        }

        std::unordered_map<int, CheckoutTuple> checkoutAggregateMap;
        for (int i = 0; i < std::min(numWindowSlides, checkoutSlideContainers.size()); i++) {
            for (auto& [key, checkoutTuple] : checkoutSlideContainers[i]) {
                if (auto it = checkoutAggregateMap.find(key); it == checkoutAggregateMap.end()) {
                    checkoutAggregateMap[key] = checkoutTuple;
                } else {
                    auto& result = it->second;
                    result.value += checkoutTuple.value;
                    result.eventTime = std::max(result.eventTime, checkoutTuple.eventTime);
                    result.processingTime = std::max(result.processingTime, checkoutTuple.processingTime);
                }
            }
        }

        // Remove old slide (if not belonging to next window anymore)
        {
            std::lock_guard lock(checkoutMutex);
            while (checkoutSlideContainers.size() > numWindowSlides) {
                checkoutSlideContainers.pop_front();
            }
        }

        // Join
        for (auto& [key, rightElement] : checkoutAggregateMap) {
            if (auto it = adAggregateMap.find(key); it != adAggregateMap.end()) {
                auto& leftElement = it->second;
                output << leftElement.adId << "," << std::fixed << rightElement.value - leftElement.cost << ","
                       << std::max(rightElement.eventTime, leftElement.eventTime) << ","
                       << std::max(rightElement.processingTime, leftElement.processingTime) << ","
                       << getCurrentUnixTimestampInMilliseconds() << "," << leftElement.numTuple << "\n";
            }
        }

        // Terminate if all windows of both streams are empty because of closed pipeline
        /*if (emptyAdWindows >= numWindowSlides && emptyCheckoutWindows >= numWindowSlides) {
            std::cout << std::flush;
            adThread.join();
            checkoutThread.join();
            output.close();
            return 0;
        }*/
    }
}
