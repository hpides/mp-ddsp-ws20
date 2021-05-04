#include <arpa/inet.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <future>
#include <getopt.h>
#include <iostream>
#include <list>
#include <mpi.h>
#include <mutex>
#include <random>
#include <thread>
#include <unistd.h>
#include <vector>

#include "fmt/chrono.h"
#include "fmt/compile.h"
#include "fmt/core.h"

const uint32_t distribution_seed = 1531445423;
const int bufferSize = 4096 * 8 * 8 * 8;

struct adStreamConfig
{
    std::pair<uint32_t, uint32_t> ad_id_range;
    std::pair<uint32_t, uint32_t> user_id_range;
    std::pair<uint32_t, uint32_t> cost_range;
};

struct checkoutStreamConfig
{
    std::pair<uint32_t, uint32_t> purchase_id_range;
    std::pair<uint32_t, uint32_t> ad_id_range;
    std::pair<uint32_t, uint32_t> user_id_range;
    std::pair<uint32_t, uint32_t> value_range;
};

class Semaphore
{
public:
    void notify()
    {
        std::lock_guard<decltype(_mutex)> lock(_mutex);
        ++_count;
        _condition.notify_one();
    }

    void wait()
    {
        std::unique_lock<decltype(_mutex)> lock(_mutex);
        while (!_count)  // Handle spurious wake-ups.
            _condition.wait(lock);
        --_count;
    }

private:
    std::mutex _mutex;
    std::condition_variable _condition;
    unsigned long _count = 0;
};


struct CommandLineOptions
{
    size_t recordCount;
    unsigned long portsBegin;
    unsigned long dataGenRate;
    size_t numThreads;
    adStreamConfig adConfig;
    checkoutStreamConfig checkoutConfig;
};

struct ProtectedBuffer
{
    explicit ProtectedBuffer(size_t recordCount, int num_threads, Semaphore* own, Semaphore* other)
        : eventTimesAppended(num_threads, 0)
        , ownSemaphore(own)
        , otherSemaphore(other)
    {
        container.reserve(recordCount);
    };

    std::mutex mutex;
    std::vector<std::string> container;
    std::condition_variable bufferFull;
    std::vector<int> eventTimesAppended;
    Semaphore* ownSemaphore;
    Semaphore* otherSemaphore;
};

inline unsigned long long getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

std::mt19937 SeedRandomGenerator(uint32_t distributionSeed)
{
    const size_t seeds_bytes = sizeof(std::mt19937::result_type) * std::mt19937::state_size;
    const size_t seeds_length = seeds_bytes / sizeof(std::seed_seq::result_type);
    std::vector<std::seed_seq::result_type> seeds(seeds_length);

    std::generate(seeds.begin(), seeds.end(), [&]() {
        distributionSeed = (distributionSeed << 1u) | (distributionSeed >> (-1u & 31u));
        return distributionSeed;
    });
    std::seed_seq seed_sequence(seeds.begin(), seeds.end());
    return std::mt19937{seed_sequence};
}

std::string generateAdByteStream(const CommandLineOptions& options)
{
    static std::uniform_int_distribution<int> ad_id_distribution(options.adConfig.ad_id_range.first, options.adConfig.ad_id_range.second);
    static std::uniform_int_distribution<int> user_id_distribution(
        options.adConfig.user_id_range.first, options.adConfig.user_id_range.second);
    static std::uniform_real_distribution<double> cost_distribution(options.adConfig.cost_range.first, options.adConfig.cost_range.second);
    static std::mt19937 random_generator = SeedRandomGenerator(distribution_seed);

    int ad_id = ad_id_distribution(random_generator);
    int user_id = user_id_distribution(random_generator);
    double cost = cost_distribution(random_generator);

    return fmt::format(FMT_COMPILE("{},{},{},{{}}"), ad_id, user_id, cost);
}

std::string generateCheckoutByteStream(const CommandLineOptions& options)
{
    static std::uniform_int_distribution<int> purchase_id_distribution(
        options.checkoutConfig.purchase_id_range.first, options.checkoutConfig.purchase_id_range.second);
    static std::uniform_int_distribution<int> ad_id_distribution(
        options.checkoutConfig.ad_id_range.first, options.checkoutConfig.ad_id_range.second);
    static std::uniform_int_distribution<int> user_id_distribution(
        options.checkoutConfig.user_id_range.first, options.checkoutConfig.user_id_range.second);
    static std::uniform_real_distribution<double> value_distribution(
        options.checkoutConfig.value_range.first, options.checkoutConfig.value_range.second);
    static std::mt19937 random_generator = SeedRandomGenerator(distribution_seed);

    int purchase_id = purchase_id_distribution(random_generator);
    int ad_id = ad_id_distribution(random_generator);
    int user_id = user_id_distribution(random_generator);
    double value = value_distribution(random_generator);

    return fmt::format(FMT_COMPILE("{},{},{},{},{{}}"), purchase_id, user_id, ad_id, value);
}

int createServerSocket(int port)
{
    // Create server socket
    int socketDesc = socket(AF_INET, SOCK_STREAM, 0);
    if (socketDesc == -1) {
        std::cerr << "ERROR: Opening socket failed.\n";
        exit(2);
    }

    // Increase TCP send buffer size
    if (setsockopt(socketDesc, SOL_SOCKET, SO_SNDBUF, &bufferSize, sizeof(bufferSize)) == -1) {
        std::cerr << "ERROR: Setting send buffer size failed.\n";
    }

    // Prevents "Address already used"-error on restart
    int option = 1;
    setsockopt(socketDesc, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));

    sockaddr_in server_addr{}, client_addr{};

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

void appendEventTimes(const CommandLineOptions& options, ProtectedBuffer& buffer, int start, int increment, int numThreads)
{
    uint64_t bytesPerChunk = 0;
    unsigned long long sleep_for = 0;

    for (int i = start; i < options.recordCount; i += increment) {

        auto t1 = std::chrono::steady_clock::now();

        // Append even time stamp to buffer string
        buffer.container[i] = fmt::format(buffer.container[i], getCurrentUnixTimestampInMilliseconds());
        bytesPerChunk += buffer.container[i].length();
        buffer.eventTimesAppended[start] = i;

        // Done appending the event time stamp to enough elements for one full tcp buffer
        if (start == 0 && bytesPerChunk * numThreads >= bufferSize) {
            bytesPerChunk = 0;
            buffer.bufferFull.notify_one();
        }

        auto t2 = std::chrono::steady_clock::now();

        if (options.dataGenRate == 0)
            continue;

        auto sleep_slice = static_cast<unsigned long long>(buffer.container[i].length() / (options.dataGenRate * 1.0) * 1000.0);

        std::chrono::duration<double> t = t2 - t1;
        auto compute_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t).count();

        // Avoid branch. Line is equivalent to: if (sleep_slice > compute_time) sleep_for += sleep_slice - compute_time;
        sleep_for += (sleep_slice > compute_time) * (sleep_slice - compute_time);

        if (sleep_for * numThreads > 10 * 1000000) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_for * numThreads));
            sleep_for = 0;
        }
    }

    buffer.eventTimesAppended[start] = options.recordCount;
    buffer.bufferFull.notify_one();
}

void produce(
    const CommandLineOptions& options,
    ProtectedBuffer& buffer,
    std::function<std::string(const CommandLineOptions&)> const& generator)
{
    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Starting to produce data..." << std::endl;

    for (size_t i = 0; i < options.recordCount; i++) {
        buffer.container[i] = std::move(generator(options));
    }

    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Done pre-generating the tuples!" << std::endl;

    buffer.ownSemaphore->notify();
    buffer.otherSemaphore->wait();

    // Use multiple threads to append the event times
    std::vector<std::thread> threads;

    for (int i = 0; i < options.numThreads; i++) {
        threads.emplace_back(appendEventTimes, options, std::ref(buffer), i, options.numThreads, options.numThreads);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Producer done!" << std::endl;
}

void consume(int socket, const CommandLineOptions& options, ProtectedBuffer& buffer)
{
    char* sendBuffer = (char*)malloc(bufferSize * sizeof(char));

    int minElementTimesAppendedIndex = 0;
    int bufferWritePointer = 0;
    uint64_t totalBytesSent = 0;
    int i = 0;
    int n = 1;
    int avg = 0;

    while (i < options.recordCount) {
        std::unique_lock<std::mutex> lock(buffer.mutex);
        buffer.bufferFull.wait(lock, [&buffer, &options, &i, &minElementTimesAppendedIndex] {
            minElementTimesAppendedIndex = *std::min_element(buffer.eventTimesAppended.begin(), buffer.eventTimesAppended.end());
            return i < minElementTimesAppendedIndex || minElementTimesAppendedIndex == options.recordCount;
        });
        lock.unlock();

        while (i < options.recordCount && bufferWritePointer + 1 + buffer.container[i].length() < bufferSize
               && i < minElementTimesAppendedIndex) {

            strncpy(sendBuffer + bufferWritePointer, buffer.container[i].c_str(), buffer.container[i].length());
            bufferWritePointer += buffer.container[i].length();

            strncpy(sendBuffer + bufferWritePointer, "\n", 1);
            bufferWritePointer++;

            i++;
        }

        totalBytesSent += bufferWritePointer;
        int res = write(socket, sendBuffer, bufferWritePointer);
        avg = avg + (bufferWritePointer - avg) / n;
        bufferWritePointer = 0;
        n++;

        if (res == -1) {
            std::cerr << "Thread #" << std::this_thread::get_id() << ": "
                      << "Error. Sending bytes via socket stream failed: " << strerror(errno) << "\n";
            close(socket);
            exit(EXIT_FAILURE);
        }
    }

    close(socket);

    std::cout << "Average TCP send buffer size: " << avg << " bytes" << std::endl;

    std::cout << "Thread #" << std::this_thread::get_id() << ": "
              << "Consumer finished! Sent " << i << " tuples and " << totalBytesSent << " bytes successfully\n";
}

void print_usage()
{
    printf("Usage: generator --recordCount <number> --portsBegin <port-id> --dataGenRate <MB/sec> --numThreads <number> [ optional: "
           "--adConfigFile <fileName> --checkoutConfigFile <filename> ]\n");
}

CommandLineOptions parse_arguments(int argc, char* argv[])
{
    static struct option long_options[] = {{"recordCount", required_argument, 0, 'c'},
                                           {"portsBegin", required_argument, 0, 'p'},
                                           {"dataGenRate", required_argument, 0, 'r'},
                                           {"numThreads", required_argument, 0, 'n'},
                                           {"adConfigFile", required_argument, 0, 'a'},
                                           {"checkoutConfigFile", required_argument, 0, 'f'},
                                           {0, 0, 0, 0}};

    CommandLineOptions options = {.recordCount = 0, .portsBegin = 0, .dataGenRate = 0, .numThreads = 0};

    std::string adConfigFileName;
    std::string checkoutConfigFileName;

    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "cpq:sr", long_options, &option_index)) != -1) {
        switch (opt) {
        case 'c':
            options.recordCount = std::stoull(optarg);
            break;
        case 'p':
            options.portsBegin = std::stoul(optarg);
            break;
        case 'r':
            options.dataGenRate = std::stoul(optarg);
            break;
        case 'n':
            options.numThreads = std::stoull(optarg);
            break;
        case 'a':
            adConfigFileName = optarg;
            break;
        case 'f':
            checkoutConfigFileName = optarg;
            break;
        default:
            print_usage();
            exit(EXIT_FAILURE);
        }
    }

    if (options.recordCount == 0 || options.portsBegin == 0 || options.numThreads == 0) {
        print_usage();
        exit(EXIT_FAILURE);
    }

    if (!adConfigFileName.empty()) {
        std::string line;
        std::ifstream adConfigFile(adConfigFileName);
        auto ptr = reinterpret_cast<uint32_t*>(&options.adConfig);

        if (adConfigFile.is_open()) {

            std::getline(adConfigFile, line);
            while (std::getline(adConfigFile, line, ',')) {
                if (line.empty())
                    break;
                *ptr = std::stoi(line);
                ptr++;
            }

            adConfigFile.close();
        }
    } else {
        options.adConfig.ad_id_range = {1, 1000};
        options.adConfig.user_id_range = {0, 100};
        options.adConfig.cost_range = {10, 100};
    }

    if (!checkoutConfigFileName.empty()) {
        std::string line;
        std::ifstream checkoutConfigFile(checkoutConfigFileName);
        auto ptr = reinterpret_cast<uint32_t*>(&options.checkoutConfig);

        if (checkoutConfigFile.is_open()) {

            std::getline(checkoutConfigFile, line);
            while (std::getline(checkoutConfigFile, line, ',')) {
                if (line.empty())
                    break;
                *ptr = std::stoi(line);
                ptr++;
            }

            checkoutConfigFile.close();
        }
    } else {
        options.checkoutConfig.purchase_id_range = {0, 10000};
        options.checkoutConfig.ad_id_range = {0, 1000};
        options.checkoutConfig.user_id_range = {0, 100};
        options.checkoutConfig.value_range = {10, 100};
    }

    return options;
}

int main(int argc, char* argv[])
{
    int providedThreadSupportLevel;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &providedThreadSupportLevel);
    int MPISize, MPIRank;
    MPI_Comm_rank(MPI_COMM_WORLD, &MPIRank);
    MPI_Comm_size(MPI_COMM_WORLD, &MPISize);

    auto options = parse_arguments(argc, argv);

    Semaphore adSemaphore, checkoutSemaphore;

    ProtectedBuffer bufferAd{options.recordCount, options.numThreads, &adSemaphore, &checkoutSemaphore},
        bufferCheckout{options.recordCount, options.numThreads, &checkoutSemaphore, &adSemaphore};

    auto socketAdTaskFuture =
        std::async(std::launch::async, [&] { return createServerSocket(static_cast<int>(options.portsBegin + 2 * MPIRank)); });
    auto socketCheckoutTaskFuture =
        std::async(std::launch::async, [&] { return createServerSocket(static_cast<int>(options.portsBegin + 2 * MPIRank + 1)); });

    int socketAd = socketAdTaskFuture.get();
    int socketCheckout = socketCheckoutTaskFuture.get();

    std::vector<std::thread> producers;
    producers.emplace_back(produce, options, std::ref(bufferAd), std::ref(generateAdByteStream));
    producers.emplace_back(produce, options, std::ref(bufferCheckout), std::ref(generateCheckoutByteStream));

    std::vector<std::thread> consumers;
    consumers.emplace_back(consume, socketAd, options, std::ref(bufferAd));
    consumers.emplace_back(consume, socketCheckout, options, std::ref(bufferCheckout));

    for (auto& producer : producers)
        producer.join();

    for (auto& consumer : consumers) {
        consumer.join();
    }

    close(socketAd);
    close(socketCheckout);

    MPI_Finalize();
    return 0;
}
