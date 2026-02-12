cmake_minimum_required(VERSION 3.14)

project(TinkoffInvestSDKTests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

# Find required packages
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

# Include parent directories for proto headers
include_directories(
    ${CMAKE_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

# Find gmock and add include path
find_package(GTest REQUIRED)
include_directories(SYSTEM ${GTEST_INCLUDE_DIRS})

# NOTE: This project was designed with each test file as a standalone executable
# Building all tests together causes linker errors due to multiple main() definitions
# 
# To run individual tests, build them separately or use:
#   cd build && make test_api_tokens
#   cd build && make test_integration_real
#   cd build && make test_streaming
#   cd build && make test_streaming_instrument_id
#   cd build && make test_streaming_payload_type
#
# Each test has its own main() and was not designed to be linked together.

# For demonstration, we build only test_api_tokens as a simple test executable
# (it doesn't use gtest and has minimal dependencies)
add_executable(test_api_tokens test_api_tokens.cpp)

target_link_libraries(test_api_tokens
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

# Streaming tests using gtest
add_executable(test_streaming test_streaming.cpp)

target_link_libraries(test_streaming
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        ${GTEST_LIBRARIES}
        ${GTEST_MAIN_LIBRARIES}
        Threads::Threads
)

# Add test to CTest
add_test(NAME test_streaming COMMAND test_streaming)

# Simple streaming test (non-gtest)
add_executable(test_streaming_simple test_streaming_simple.cpp)

target_link_libraries(test_streaming_simple
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

