cmake_minimum_required(VERSION 3.10)
project(streaming_engine)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if (APPLE)
    set(CMAKE_C_COMPILER "/usr/local/bin/gcc-10")
    set(CMAKE_CXX_COMPILER "/usr/local/bin/g++-10")
endif ()

set(SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/OperatorPipe.h)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/operators)

add_executable(streaming_engine main.cpp ${SOURCE_FILES})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(streaming_engine PRIVATE Threads::Threads)

# testing
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests/googletest)
enable_testing()
include_directories(${gtest_SOURCE_DIR}${gtest_SOURCE_DIR}/include)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
add_executable(unit_tests tests/main.cpp ${TEST_SOURCE_FILES})
target_link_libraries(unit_tests PRIVATE Threads::Threads gtest gtest_main)
