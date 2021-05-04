cmake_minimum_required(VERSION 3.10)
project(distributed_hardcoded_streaming_engine)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_executable(distributed_hardcoded_streaming_engine main.cpp)

if(CI_SERVER)
    set(CMAKE_PREFIX_PATH /usr)
else()
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/openmpi)
endif()

find_package(MPI COMPONENTS CXX OPTIONAL_COMPONENTS)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(distributed_hardcoded_streaming_engine PRIVATE Threads::Threads ${MPI_CXX_LIBRARIES})

# Execute with:
# /usr/mpi/gcc/openmpi-4.1.0rc5/bin/mpirun -np 4 --host node-01,node-02,node-03,node-04 build/hardcoded_streaming_engine
