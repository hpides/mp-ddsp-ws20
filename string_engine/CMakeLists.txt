cmake_minimum_required(VERSION 3.10)
project(string_engine)

#if (APPLE)
#    set(CMAKE_C_COMPILER "/usr/local/bin/gcc-10")
#    set(CMAKE_CXX_COMPILER "/usr/local/bin/g++-10")
#endif()

find_package(Threads REQUIRED)

add_subdirectory(fmt)
set(CMAKE_PREFIX_PATH fmt)

set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/operator)

set(SOURCE_FILES ${OPERATOR_SOURCE_FILES}
        ${CMAKE_CURRENT_SOURCE_DIR}/Pipeline.cpp Schema.cpp)

include_directories(string_engine fmt/include/)
add_executable(string_engine main.cpp ${SOURCE_FILES})
target_link_libraries(string_engine PRIVATE Threads::Threads fmt::fmt)

#if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cmake-build-debug/compiled_queries/query.cpp")
#    add_executable(query cmake-build-debug/compiled_queries/query.cpp)
#    target_link_libraries(query PRIVATE Threads::Threads)
#endif()
