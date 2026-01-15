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

# Create test executable for API token tests (simpler, no gmock dependencies)
add_executable(test_api_tokens test_api_tokens.cpp)

target_link_libraries(test_api_tokens
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

# Create streaming sandbox test executable
add_executable(test_streaming_sandbox test_streaming_sandbox.cpp)

target_link_libraries(test_streaming_sandbox
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

# Collect all test source files (excluding test_api_tokens.cpp which is built separately)
file(GLOB TEST_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    EXCLUDE
        "${CMAKE_CURRENT_SOURCE_DIR}/test_api_tokens.cpp"
)

# Create test executable
add_executable(${PROJECT_NAME} ${TEST_SOURCES})

target_link_libraries(${PROJECT_NAME}
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        GTest::GTest
        GTest::Main
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

# Enable testing
enable_testing()
add_test(NAME ${PROJECT_NAME} COMMAND ${PROJECT_NAME})

# Copy test data files if any

