cmake_minimum_required(VERSION 3.14)

project(TinkoffInvestSDKTests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
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

# Last price streaming tests (gtest)
add_executable(test_streaming_last_price test_streaming_last_price.cpp)

target_link_libraries(test_streaming_last_price
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
add_test(NAME test_streaming_last_price COMMAND test_streaming_last_price)

# Order book streaming tests (gtest)
add_executable(test_streaming_orderbook test_streaming_orderbook.cpp)

target_link_libraries(test_streaming_orderbook
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
add_test(NAME test_streaming_orderbook COMMAND test_streaming_orderbook)

# Trade streaming tests (gtest)
add_executable(test_streaming_trades test_streaming_trades.cpp)

target_link_libraries(test_streaming_trades
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
add_test(NAME test_streaming_trades COMMAND test_streaming_trades)

# Trading info (SubscribeInfoAsync) streaming tests (gtest)
add_executable(test_streaming_info test_streaming_info.cpp)

target_link_libraries(test_streaming_info
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
add_test(NAME test_streaming_info COMMAND test_streaming_info)

# Candle streaming tests (gtest)
add_executable(test_streaming_candles test_streaming_candles.cpp)

target_link_libraries(test_streaming_candles
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
add_test(NAME test_streaming_candles COMMAND test_streaming_candles)

# Independent streams test - verifies each subscription creates its own stream
add_executable(test_streaming_independent_streams test_streaming_independent_streams.cpp)

target_link_libraries(test_streaming_independent_streams
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
add_test(NAME test_streaming_independent_streams COMMAND test_streaming_independent_streams)

# Streaming callback nullptr fix test - verifies that streaming callbacks 
# return valid data via getMarketDataStreamResponse() instead of nullptr from ptr()
add_executable(test_streaming_callback_nullptr_fix test_streaming_callback_nullptr_fix.cpp)

target_link_libraries(test_streaming_callback_nullptr_fix
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
add_test(NAME test_streaming_callback_nullptr_fix COMMAND test_streaming_callback_nullptr_fix)

# MarketDataStream Subscribe and Wait 10 Minutes Test (gtest)
# This test subscribes to market data stream and waits 10 minutes,
# logging detailed information for each incoming message
add_executable(test_streaming_subscribe_wait_10min test_streaming_subscribe_wait_10min.cpp)

target_link_libraries(test_streaming_subscribe_wait_10min
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
add_test(NAME test_streaming_subscribe_wait_10min COMMAND test_streaming_subscribe_wait_10min)

# Simple OrderBook Streaming Test (gtest) - demonstrates ORDERBOOK subscription
add_executable(test_streaming_orderbook_simple test_streaming_orderbook_simple.cpp)

target_link_libraries(test_streaming_orderbook_simple
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
add_test(NAME test_streaming_orderbook_simple COMMAND test_streaming_orderbook_simple)

# New Functionality Tests - Tests for new SDK methods
# Tests: FindInstrument, GetCountries, GetBrandBy, OptionBy, Options, PostOrderAsync, PostSandboxOrderAsync
add_executable(test_new_functionality test_new_functionality.cpp)

target_link_libraries(test_new_functionality
    PRIVATE
        tink_grpc_proto
        TinkoffInvestSDK
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Threads::Threads
)

# Add test to CTest
add_test(NAME test_new_functionality COMMAND test_new_functionality)

# OperationsStream Tests - Tests for OperationsStream service (Portfolio and Positions streaming)
add_executable(test_operations_stream test_operations_stream.cpp)

target_link_libraries(test_operations_stream
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
add_test(NAME test_operations_stream COMMAND test_operations_stream)

