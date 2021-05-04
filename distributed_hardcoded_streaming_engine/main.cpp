#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <mpi.h>
#include <mutex>
#include <numeric>
#include <queue>
#include <thread>
#include <unordered_map>

const size_t bufferSize = 4096 * 8 * 8 * 8;

inline uint64_t getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

struct MPIConfig
{
    int MPIRank, MPISize, providedThreadSupportLevel;
};

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
    uint8_t numDecimals = 1;
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

void runAdThreadTCPReceiver(const int adInputSocket, std::unordered_map<int, AdTuple>& adAggregateMap, std::mutex& adMutex)
{
    char buffer[bufferSize];
    std::string surplus;

    while (true) {
        strncpy(buffer, surplus.c_str(), surplus.size());

        int bytesRead = recv(adInputSocket, buffer + surplus.size(), bufferSize - surplus.size(), 0);
        bytesRead += surplus.size();

        if (bytesRead == -1) {
            std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
            exit(6);
        } else if (bytesRead == 0) {
            throw std::runtime_error("end of pipeline");
            /*isAdError = true;
            break;*/
        }

        int nextStart = 0;
        for (int i = 0; i < bytesRead; i++) {
            if (buffer[i] == '\n') {
                int tupleLength = i - nextStart;
                char* pos = buffer + nextStart;
                char* end = pos + tupleLength;
                nextStart = i + 1;

                AdTuple adTuple{
                    .adId = parse<int>(pos),
                    .userId = parse<int>(pos),
                    .cost = parse<double>(pos),
                    .eventTime = parse<uint64_t>(pos, end),
                    .processingTime = getCurrentUnixTimestampInMilliseconds(),
                    .numTuple = 1};

                std::lock_guard lock(adMutex);
                if (auto it = adAggregateMap.find(adTuple.adId); it == adAggregateMap.end()) {
                    adAggregateMap[adTuple.adId] = adTuple;
                } else {
                    auto& result = it->second;
                    result.cost += adTuple.cost;
                    result.eventTime = std::max(result.eventTime, adTuple.eventTime);
                    result.processingTime = std::max(result.processingTime, adTuple.processingTime);
                    result.numTuple += adTuple.numTuple;
                }

            }
        }

        surplus.assign(buffer + nextStart, bytesRead - nextStart);
    }
}

void runCheckoutThreadTCPReceiver(
    const int checkoutInputSocket,
    std::unordered_map<int, CheckoutTuple>& checkoutAggregateMap,
    std::mutex& checkoutMutex)
{
    char buffer[bufferSize];
    std::string surplus;

    while (true) {
        strncpy(buffer, surplus.c_str(), surplus.size());

        int bytesRead = recv(checkoutInputSocket, buffer + surplus.size(), bufferSize - surplus.size(), 0);
        bytesRead += surplus.size();

        if (bytesRead == -1) {
            std::cerr << "ERROR: Receiving bytes from server failed: " << strerror(errno) << "\n";
            exit(6);
        } else if (bytesRead == 0) {
            throw std::runtime_error("end of pipeline");
            /*isCheckoutError = true;
            break;*/
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

                if (checkoutTuple.adId == 0)
                    continue;

                std::lock_guard lock(checkoutMutex);
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

        surplus.assign(buffer + nextStart, bytesRead - nextStart);
    }
}

int main(int argc, char** argv)
{
    // MPI Setup
    MPIConfig mpiConfig{};
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &mpiConfig.providedThreadSupportLevel);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpiConfig.MPIRank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpiConfig.MPISize);

    auto adInputSocketFuture = std::async(std::launch::async, [&] { return connectToServerSocket(12345 + 2 * mpiConfig.MPIRank); });
    auto checkoutInputSocketFuture =
        std::async(std::launch::async, [&] { return connectToServerSocket(12345 + 2 * mpiConfig.MPIRank + 1); });

    int adInputSocket, checkoutInputSocket;
    adInputSocket = adInputSocketFuture.get();
    checkoutInputSocket = checkoutInputSocketFuture.get();

    std::chrono::milliseconds windowSizeMillis(5000);
    std::chrono::milliseconds windowSlideSizeMillis(1000);
    auto numWindowSlides = static_cast<std::deque<int>::size_type>(windowSizeMillis / windowSlideSizeMillis);
    std::unordered_map<int, AdTuple> adAggregateMap;
    std::unordered_map<int, CheckoutTuple> checkoutAggregateMap;
    std::deque<std::unordered_map<int, AdTuple>> rankSpecificAdSlideContainers(1);
    std::deque<std::unordered_map<int, CheckoutTuple>> rankSpecificCheckoutSlideContainers(1);
    std::mutex adMutex, checkoutMutex;
    std::condition_variable sendCondition;
    std::chrono::time_point<std::chrono::high_resolution_clock> currentSlideStartedTime = std::chrono::high_resolution_clock::now();
    /*bool isAdError = false, isCheckoutError = false;
    int emptyAdWindows = 0, emptyCheckoutWindows = 0;*/
    std::ofstream output("output" + std::to_string(mpiConfig.MPIRank) + ".csv", std::ios::out | std::ios::trunc);

    // In a separate thread, collect elements from the pipeline into the new slide
    std::thread adThreadTCPReceiver([&] { runAdThreadTCPReceiver(adInputSocket, adAggregateMap, adMutex); });
    std::thread checkoutThreadTCPReceiver([&] { runCheckoutThreadTCPReceiver(checkoutInputSocket, checkoutAggregateMap, checkoutMutex); });

    while (true) {
        // In main thread, wait until current slide is over
        currentSlideStartedTime += windowSlideSizeMillis;
        if (mpiConfig.MPIRank == 0)
            std::this_thread::sleep_until(std::chrono::time_point(currentSlideStartedTime));

        MPI_Barrier(MPI_COMM_WORLD);

        // AdStream
        std::vector<std::vector<AdTuple>> adTuplesByRank(mpiConfig.MPISize);
        std::vector<int> sendTupleAmountsByRankAd, sendTupleStartIndicesAd;
        AdTuple* adTupleSendBuffer;
        {
            std::lock_guard lock(adMutex);
            for (auto& [key, adTuple] : adAggregateMap) {
                adTuplesByRank[key % mpiConfig.MPISize].push_back(adTuple);
            }

            adTupleSendBuffer = (AdTuple*)malloc(adAggregateMap.size() * sizeof(AdTuple));
            int startIndex = 0;
            for (auto& tuples : adTuplesByRank) {
                std::copy(tuples.begin(), tuples.end(), adTupleSendBuffer + startIndex);
                sendTupleAmountsByRankAd.push_back(tuples.size() * sizeof(AdTuple));
                sendTupleStartIndicesAd.push_back(startIndex * sizeof(AdTuple));
                startIndex += tuples.size();
            }

            adAggregateMap.clear();
        }

        std::vector<int> recvTupleAmountsByRankAd(mpiConfig.MPISize);
        MPI_Alltoall(sendTupleAmountsByRankAd.data(), 1, MPI_INT32_T, recvTupleAmountsByRankAd.data(), 1, MPI_INT32_T, MPI_COMM_WORLD);

        int tuplesToReceiveAd = std::accumulate(recvTupleAmountsByRankAd.begin(), recvTupleAmountsByRankAd.end(), 0);
        auto* adTupleRecvBuffer = (AdTuple*)malloc(tuplesToReceiveAd);
        std::vector<int> recvTupleStartIndices;
        int startIndexAd = 0;
        for (auto& amountTuples : recvTupleAmountsByRankAd) {
            recvTupleStartIndices.push_back(startIndexAd);
            startIndexAd += amountTuples;
        }
        MPI_Alltoallv(
            adTupleSendBuffer,
            sendTupleAmountsByRankAd.data(),
            sendTupleStartIndicesAd.data(),
            MPI_BYTE,
            adTupleRecvBuffer,
            recvTupleAmountsByRankAd.data(),
            recvTupleStartIndices.data(),
            MPI_BYTE,
            MPI_COMM_WORLD);

        free(adTupleSendBuffer);

        for (int i = 0; i < tuplesToReceiveAd / sizeof(AdTuple); i++) {
            auto& rankSpecificCurrentSlideAdAggregateMap = rankSpecificAdSlideContainers.back();
            AdTuple* adTuple = &adTupleRecvBuffer[i];
            int key = adTuple->adId;

            if (auto it = rankSpecificCurrentSlideAdAggregateMap.find(key); it == rankSpecificCurrentSlideAdAggregateMap.end())
                rankSpecificCurrentSlideAdAggregateMap[key] = *adTuple;
            else {
                auto& result = it->second;
                result.cost += adTuple->cost;
                result.eventTime = std::max(result.eventTime, adTuple->eventTime);
                result.processingTime = std::max(result.processingTime, adTuple->processingTime);
                result.numTuple += adTuple->numTuple;
            }
        }

        free(adTupleRecvBuffer);
        std::unordered_map<int, AdTuple> rankSpecificWindowedAdAggregateMap;

        /*if (isAdError)
            emptyAdWindows++;*/

        // Aggregate part combined with collection from Slides
        for (int i = 0; i < rankSpecificAdSlideContainers.size(); i++) {
            for (auto& [key, adTuple] : rankSpecificAdSlideContainers[i]) {
                if (auto it = rankSpecificWindowedAdAggregateMap.find(key); it == rankSpecificWindowedAdAggregateMap.end())
                    rankSpecificWindowedAdAggregateMap[key] = adTuple;
                else {
                    auto& result = it->second;
                    result.cost += adTuple.cost;
                    result.eventTime = std::max(result.eventTime, adTuple.eventTime);
                    result.processingTime = std::max(result.processingTime, adTuple.processingTime);
                    result.numTuple += adTuple.numTuple;
                }
            }
        }

        rankSpecificAdSlideContainers.emplace_back();
        // Remove old slide (if not belonging to next window anymore)
        while (rankSpecificAdSlideContainers.size() > numWindowSlides)
            rankSpecificAdSlideContainers.pop_front();

        // CheckoutStream
        std::vector<std::vector<CheckoutTuple>> checkoutTuplesByRank(mpiConfig.MPISize);
        std::vector<int> sendTupleAmountsByRankCheckout, sendTupleStartIndicesCheckout;
        CheckoutTuple* checkoutTupleSendBuffer;
        {
            std::lock_guard lock(checkoutMutex);
            for (auto& [key, checkoutTuple] : checkoutAggregateMap)
                checkoutTuplesByRank[key % mpiConfig.MPISize].push_back(checkoutTuple);

            checkoutTupleSendBuffer = (CheckoutTuple*)malloc(checkoutAggregateMap.size() * sizeof(CheckoutTuple));
            int startIndex = 0;
            for (auto& tuples : checkoutTuplesByRank) {
                std::copy(tuples.begin(), tuples.end(), checkoutTupleSendBuffer + startIndex);
                sendTupleAmountsByRankCheckout.push_back(tuples.size() * sizeof(CheckoutTuple));
                sendTupleStartIndicesCheckout.push_back(startIndex * sizeof(CheckoutTuple));
                startIndex += tuples.size();
            }

            checkoutAggregateMap.clear();
        }

        std::vector<int> recvTupleAmountsByRankCheckout(mpiConfig.MPISize);
        MPI_Alltoall(
            sendTupleAmountsByRankCheckout.data(), 1, MPI_INT32_T, recvTupleAmountsByRankCheckout.data(), 1, MPI_INT32_T, MPI_COMM_WORLD);

        int tuplesToReceiveCheckout = std::accumulate(recvTupleAmountsByRankCheckout.begin(), recvTupleAmountsByRankCheckout.end(), 0);
        auto* checkoutTupleRecvBuffer = (CheckoutTuple*)malloc(tuplesToReceiveCheckout);
        std::vector<int> recvTupleStartIndicesCheckout;
        int startIndexCheckout = 0;
        for (auto& amountTuples : recvTupleAmountsByRankCheckout) {
            recvTupleStartIndices.push_back(startIndexCheckout);
            startIndexCheckout += amountTuples;
        }
        MPI_Alltoallv(
            checkoutTupleSendBuffer,
            sendTupleAmountsByRankCheckout.data(),
            sendTupleStartIndicesCheckout.data(),
            MPI_BYTE,
            checkoutTupleRecvBuffer,
            recvTupleAmountsByRankCheckout.data(),
            recvTupleStartIndices.data(),
            MPI_BYTE,
            MPI_COMM_WORLD);

        free(checkoutTupleSendBuffer);

        for (int i = 0; i < tuplesToReceiveCheckout / sizeof(CheckoutTuple); i++) {
            auto& rankSpecificCurrentSlideCheckoutAggregateMap = rankSpecificCheckoutSlideContainers.back();
            CheckoutTuple* checkoutTuple = &checkoutTupleRecvBuffer[i];
            int key = checkoutTuple->adId;

            if (auto it = rankSpecificCurrentSlideCheckoutAggregateMap.find(key); it == rankSpecificCurrentSlideCheckoutAggregateMap.end())
                rankSpecificCurrentSlideCheckoutAggregateMap[key] = *checkoutTuple;
            else {
                auto& result = it->second;
                result.value += checkoutTuple->value;
                result.eventTime = std::max(result.eventTime, checkoutTuple->eventTime);
                result.processingTime = std::max(result.processingTime, checkoutTuple->processingTime);
            }
        }

        free(checkoutTupleRecvBuffer);
        std::unordered_map<int, CheckoutTuple> rankSpecificWindowedCheckoutAggregateMap;

        /*if (isCheckoutError)
            emptyCheckoutWindows++;*/

        // Aggregate part combined with collection from Slides
        for (int i = 0; i < rankSpecificCheckoutSlideContainers.size(); i++) {
            for (auto& [key, checkoutTuple] : rankSpecificCheckoutSlideContainers[i]) {
                if (auto it = rankSpecificWindowedCheckoutAggregateMap.find(key); it == rankSpecificWindowedCheckoutAggregateMap.end())
                    rankSpecificWindowedCheckoutAggregateMap[key] = checkoutTuple;
                else {
                    auto& result = it->second;
                    result.value += checkoutTuple.value;
                    result.eventTime = std::max(result.eventTime, checkoutTuple.eventTime);
                    result.processingTime = std::max(result.processingTime, checkoutTuple.processingTime);
                }
            }
        }

        rankSpecificCheckoutSlideContainers.emplace_back();
        // Remove old slide (if not belonging to next window anymore)
        while (rankSpecificCheckoutSlideContainers.size() > numWindowSlides)
            rankSpecificCheckoutSlideContainers.pop_front();

        // Join
        for (auto& [key, rightElement] : rankSpecificWindowedCheckoutAggregateMap) {
            if (auto it = rankSpecificWindowedAdAggregateMap.find(key); it != rankSpecificWindowedAdAggregateMap.end()) {
                auto& leftElement = it->second;
                output << leftElement.adId << "," << std::fixed << rightElement.value - leftElement.cost << ","
                       << std::max(rightElement.eventTime, leftElement.eventTime) << ","
                       << std::max(rightElement.processingTime, leftElement.processingTime) << ","
                       << getCurrentUnixTimestampInMilliseconds() << "," << leftElement.numTuple << "\n";
            }
        }
        output << std::flush;

        // Terminate if all windows of both streams are empty because of closed pipeline
        /*if (emptyAdWindows >= numWindowSlides && emptyCheckoutWindows >= numWindowSlides) {
            std::cout << std::flush;
            output.close();
            adThreadTCPReceiver.join();
            checkoutThreadTCPReceiver.join();
            break;
        }*/
    }

    MPI_Finalize();
    return 0;
}
