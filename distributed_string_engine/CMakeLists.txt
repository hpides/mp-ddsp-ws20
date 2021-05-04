cmake_minimum_required(VERSION 3.10)
project(distributed_string_engine)

find_package(Threads REQUIRED)

add_subdirectory(fmt)
set(CMAKE_PREFIX_PATH fmt)

set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/operator)

set(SOURCE_FILES ${OPERATOR_SOURCE_FILES}
        ${CMAKE_CURRENT_SOURCE_DIR}/Pipeline.cpp Schema.cpp)

include_directories(distributed_string_engine fmt/include/)
add_executable(distributed_string_engine main.cpp ${SOURCE_FILES})
target_link_libraries(distributed_string_engine PRIVATE Threads::Threads fmt::fmt)

if(CI_SERVER)
    set(CMAKE_PREFIX_PATH /usr)
else()
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/openmpi)
endif()

find_package(MPI COMPONENTS CXX OPTIONAL_COMPONENTS)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cmake-build-debug/compiled_queries/query.cpp")
    include_directories(${CMAKE_SOURCE_DIR}/generated_code_libraries/)
    set(QUERY_SOURCE_FILES ${CMAKE_SOURCE_DIR}/generated_code_libraries/Semaphore.h ${CMAKE_SOURCE_DIR}/generated_code_libraries/Timestamp.h)
    add_executable(query cmake-build-debug/compiled_queries/query.cpp ${QUERY_SOURCE_FILES})
    target_link_libraries(query PRIVATE Threads::Threads ${MPI_CXX_LIBRARIES})
endif()
