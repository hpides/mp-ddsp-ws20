cmake_minimum_required(VERSION 3.10)
project(hardcoded_streaming_engine)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

#if (APPLE)
#    set(CMAKE_C_COMPILER "/usr/local/bin/gcc-10")
#    set(CMAKE_CXX_COMPILER "/usr/local/bin/g++-10")
#endif ()

add_executable(hardcoded_streaming_engine main.cpp)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(hardcoded_streaming_engine PRIVATE Threads::Threads)
