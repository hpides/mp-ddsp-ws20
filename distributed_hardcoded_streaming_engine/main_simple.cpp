#include <cmath>
#include <cstdint>
#include <iostream>
#include <mpi.h>
#include <mutex>
#include <thread>

inline uint64_t getCurrentUnixTimestampInMilliseconds()
{
    const auto t = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

struct MPIConfig
{
    int MPIRank, MPISize, providedThreadSupportLevel;
    MPI_Datatype MPIAdTupleType, MPICheckoutTupleType;
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
    uint64_t purchaseId;
    int userId;
    int adId;
    double value;
    uint64_t eventTime;
    uint64_t processingTime;
};

enum MPI_Tag
{
    AD_TUPLE,
    CHECKOUT_TUPLE
};

void runAdThreadTCPReceiver(MPIConfig mpiConfig)
{
    uint64_t sendCount = 0;
    MPI_Request forwardingRequest;
    while (true) {

        AdTuple adTuple{
            .adId = rand(),
            .userId = 1,
            .cost = 0.5,
            .eventTime = getCurrentUnixTimestampInMilliseconds(),
            .processingTime = getCurrentUnixTimestampInMilliseconds(),
            .numTuple = 1};

        if (int designatedRank = adTuple.adId % mpiConfig.MPISize; designatedRank != mpiConfig.MPIRank) {
            if (forwardingRequest != nullptr) {
                MPI_Wait(&forwardingRequest, MPI_STATUS_IGNORE);
            }
            MPI_Isend(&adTuple, 1, mpiConfig.MPIAdTupleType, designatedRank, MPI_Tag::AD_TUPLE, MPI_COMM_WORLD, &forwardingRequest);
        }

        if (++sendCount % 100000 == 0) {
            std::cout << "Ad processed " << sendCount << " tuple." << std::endl;
        }
    }
}

void runCheckoutThreadTCPReceiver(MPIConfig mpiConfig)
{
    uint64_t sendCount = 0;
    MPI_Request forwardingRequest;
    while (true) {

        CheckoutTuple checkoutTuple{
            .purchaseId = 1,
            .userId = 1,
            .adId = rand(),
            .value = 0.5,
            .eventTime = getCurrentUnixTimestampInMilliseconds(),
            .processingTime = getCurrentUnixTimestampInMilliseconds()};

        if (int designatedRank = checkoutTuple.adId % mpiConfig.MPISize; designatedRank != mpiConfig.MPIRank) {
            if (forwardingRequest != nullptr) {
                MPI_Wait(&forwardingRequest, MPI_STATUS_IGNORE);
            }
            MPI_Isend(
                &checkoutTuple,
                1,
                mpiConfig.MPICheckoutTupleType,
                designatedRank,
                MPI_Tag::CHECKOUT_TUPLE,
                MPI_COMM_WORLD,
                &forwardingRequest);
        }

        if (++sendCount % 100000 == 0) {
            std::cout << "Checkout processed " << sendCount << " tuple." << std::endl;
        }
    }
}

void runAdThreadMPIReceiver(MPIConfig mpiConfig)
{
    AdTuple adTuple;
    std::cout << "ad Receiver says: " << sizeof(adTuple) << std::endl;
    while (true)
        MPI_Recv(&adTuple, 1, mpiConfig.MPIAdTupleType, MPI_ANY_SOURCE, MPI_Tag::AD_TUPLE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void runCheckoutThreadMPIReceiver(MPIConfig mpiConfig)
{
    CheckoutTuple checkoutTuple;
    std::cout << "checkout Receiver says: " << sizeof(checkoutTuple) << std::endl;
    while (true)
        MPI_Recv(
            &checkoutTuple, 1, mpiConfig.MPICheckoutTupleType, MPI_ANY_SOURCE, MPI_Tag::CHECKOUT_TUPLE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

MPI_Datatype createMPICheckoutTupleType()
{
    const int nitems = 6;
    int blocklengths[] = {8, 4, 4, 8, 8, 8};
    MPI_Datatype types[] = {MPI_UINT64_T, MPI_INT, MPI_INT, MPI_DOUBLE, MPI_UINT64_T, MPI_UINT64_T};
    MPI_Datatype MPICheckoutTupleType;
    MPI_Aint offsets[] = {
        offsetof(CheckoutTuple, purchaseId),
        offsetof(CheckoutTuple, userId),
        offsetof(CheckoutTuple, adId),
        offsetof(CheckoutTuple, value),
        offsetof(CheckoutTuple, eventTime),
        offsetof(CheckoutTuple, processingTime)};

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &MPICheckoutTupleType);
    MPI_Type_commit(&MPICheckoutTupleType);

    return MPICheckoutTupleType;
}

MPI_Datatype createMPIAdTupleType()
{
    const int nitems = 6;
    int blocklengths[] = {4, 4, 8, 8, 8, 8};
    MPI_Datatype types[] = {MPI_INT, MPI_INT, MPI_DOUBLE, MPI_UINT64_T, MPI_UINT64_T, MPI_UINT64_T};
    MPI_Datatype MPIAdTupleType;
    MPI_Aint offsets[] = {
        offsetof(AdTuple, adId),
        offsetof(AdTuple, userId),
        offsetof(AdTuple, cost),
        offsetof(AdTuple, eventTime),
        offsetof(AdTuple, processingTime),
        offsetof(AdTuple, numTuple)};

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &MPIAdTupleType);
    MPI_Type_commit(&MPIAdTupleType);

    return MPIAdTupleType;
}

int main(int argc, char** argv)
{
    MPIConfig mpiConfig{};
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpiConfig.providedThreadSupportLevel);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpiConfig.MPIRank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpiConfig.MPISize);
    mpiConfig.MPIAdTupleType = createMPIAdTupleType();
    mpiConfig.MPICheckoutTupleType = createMPICheckoutTupleType();

    std::thread adThreadTCPReceiver([&] { runAdThreadTCPReceiver(mpiConfig); });
    std::thread adThreadMPIReceiver([&] { runAdThreadMPIReceiver(mpiConfig); });

    std::thread checkoutThreadTCPReceiver([&] { runCheckoutThreadTCPReceiver(mpiConfig); });
    std::thread checkoutThreadMPIReceiver([&] { runCheckoutThreadMPIReceiver(mpiConfig); });

    adThreadTCPReceiver.join();
}
