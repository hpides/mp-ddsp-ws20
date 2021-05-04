cmake_minimum_required(VERSION 3.10)
project(generator)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_executable(generator main.cpp)

add_subdirectory(fmt)
set(CMAKE_PREFIX_PATH fmt)
include_directories(generator fmt/include/)

if(CI_SERVER)
    set(CMAKE_PREFIX_PATH /usr)
else()
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/openmpi)
endif()
find_package(MPI COMPONENTS CXX OPTIONAL_COMPONENTS)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

target_link_libraries(generator PRIVATE Threads::Threads fmt::fmt ${MPI_CXX_LIBRARIES})

include_directories(engine_test fmt/include/)
add_executable(engine_test engineTest.cpp)
target_link_libraries(engine_test PRIVATE Threads::Threads fmt::fmt)

add_executable(client_mock client_mock.cpp)

# Execute with:
# /usr/mpi/gcc/openmpi-4.1.0rc5/bin/mpirun -np 4 --host node-01,node-02,node-03,node-04 ./generator --recordCount 50000000 --portsBegin 12345 --dataGenRate 0 --numThreads 4
