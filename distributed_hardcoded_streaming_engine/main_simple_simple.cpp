#include <cmath>
#include <cstdint>
#include <iostream>
#include <mpi.h>
#include <thread>

struct MPIConfig
{
    int MPIRank, MPISize, providedThreadSupportLevel;
    MPI_Datatype MPIAdTupleType, MPICheckoutTupleType;
};

struct AdTuple
{
    int adId;
};

struct CheckoutTuple
{
    int adId;
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

        AdTuple adTuple{.adId = rand()};

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

        CheckoutTuple checkoutTuple{.adId = rand()};

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
    while (true)
        MPI_Recv(&adTuple, 1, mpiConfig.MPIAdTupleType, MPI_ANY_SOURCE, MPI_Tag::AD_TUPLE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void runCheckoutThreadMPIReceiver(MPIConfig mpiConfig)
{
    CheckoutTuple checkoutTuple;
    while (true)
        MPI_Recv(
            &checkoutTuple, 1, mpiConfig.MPICheckoutTupleType, MPI_ANY_SOURCE, MPI_Tag::CHECKOUT_TUPLE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

MPI_Datatype createMPICheckoutTupleType()
{
    const int nitems = 1;
    int blocklengths[] = {4};
    MPI_Datatype types[] = {MPI_INT};
    MPI_Datatype MPICheckoutTupleType;
    MPI_Aint offsets[] = {offsetof(CheckoutTuple, adId)};

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &MPICheckoutTupleType);
    MPI_Type_commit(&MPICheckoutTupleType);

    return MPICheckoutTupleType;
}

MPI_Datatype createMPIAdTupleType()
{
    const int nitems = 1;
    int blocklengths[] = {4};
    MPI_Datatype types[] = {MPI_INT};
    MPI_Datatype MPIAdTupleType;
    MPI_Aint offsets[] = {offsetof(AdTuple, adId)};

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

    while (true) {
    }
}
