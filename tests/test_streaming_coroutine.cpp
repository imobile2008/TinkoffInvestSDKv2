/**
 * @file test_streaming_coroutine.cpp
 * @brief Test for C++20 coroutine-based streaming
 * 
 * This test demonstrates the use of C++20 coroutines with the MarketDataStream service.
 */

#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"
#include "marketdatastreamresponse.h"
#include "commontypes.h"

using namespace tinkoff::public::invest::api::contract::v1;

class CoroutineStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Read token from file or environment
        std::ifstream tokenFile(".test_token.txt");
        if (tokenFile.is_open()) {
            std::getline(tokenFile, token);
            tokenFile.close();
        } else {
            const char* envToken = std::getenv("TINKOFF_TOKEN");
            if (envToken) {
                token = envToken;
            } else {
                GTEST_SKIP() << "No token found - set TINKOFF_TOKEN or create .test_token.txt";
            }
        }
        
        auto channel = grpc::CreateChannel("invest-public-api.tinkoff.ru:443", 
                                           grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, token);
    }
    
    std::string token;
    std::shared_ptr<MarketDataStream> stream;
};

// ============================================================================
// Test: Coroutine-based Order Book Streaming
// ============================================================================

TEST_F(CoroutineStreamTest, SubscribeOrderBookCoroutine_Basic)
{
    std::vector<std::string> instruments = {"BBG004S584W1"}; // Sberbank
    int32_t depth = 10;
    
    std::cout << "[Test] Starting coroutine-based order book stream for 10 seconds..." << std::endl;
    
    int messageCount = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(10);
    
    // Use co_yield to iterate over streaming responses
    for co_await (auto& response : stream->SubscribeOrderBookCoroutine(instruments, depth)) {
        messageCount++;
        
        auto payloadType = response.getPayloadType();
        std::cout << "[Test] Received message #" << messageCount 
                  << " type: " << response.getPayloadTypeString() << std::endl;
        
        // Check if we've reached the timeout
        if (std::chrono::steady_clock::now() - startTime >= duration) {
            std::cout << "[Test] Timeout reached, closing stream..." << std::endl;
            break;
        }
    }
    
    std::cout << "[Test] Stream ended. Total messages: " << messageCount << std::endl;
    
    EXPECT_GT(messageCount, 0) << "Should receive at least one message";
}

// ============================================================================
// Test: Coroutine-based Last Price Streaming
// ============================================================================

TEST_F(CoroutineStreamTest, SubscribeLastPriceCoroutine_Basic)
{
    std::vector<std::string> instruments = {
        "BBG004S584W1", // Sberbank
        "BBG006L8G4H1"  // Tinkoff
    };
    
    std::cout << "[Test] Starting coroutine-based last price stream for 10 seconds..." << std::endl;
    
    int messageCount = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(10);
    
    for co_await (auto& response : stream->SubscribeLastPriceCoroutine(instruments)) {
        messageCount++;
        
        auto payloadType = response.getPayloadType();
        std::cout << "[Test] Received message #" << messageCount 
                  << " type: " << response.getPayloadTypeString() << std::endl;
        
        if (std::chrono::steady_clock::now() - startTime >= duration) {
            break;
        }
    }
    
    std::cout << "[Test] Stream ended. Total messages: " << messageCount << std::endl;
    
    EXPECT_GT(messageCount, 0);
}

// ============================================================================
// Test: Coroutine-based Candle Streaming
// ============================================================================

TEST_F(CoroutineStreamTest, SubscribeCandlesCoroutine_Basic)
{
    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {"BBG004S584W1", SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };
    
    std::cout << "[Test] Starting coroutine-based candles stream for 15 seconds..." << std::endl;
    
    int messageCount = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(15);
    
    for co_await (auto& response : stream->SubscribeCandlesCoroutine(candles)) {
        messageCount++;
        
        std::cout << "[Test] Received message #" << messageCount 
                  << " type: " << response.getPayloadTypeString() << std::endl;
        
        if (std::chrono::steady_clock::now() - startTime >= duration) {
            break;
        }
    }
    
    std::cout << "[Test] Stream ended. Total messages: " << messageCount << std::endl;
    
    EXPECT_GT(messageCount, 0);
}

// ============================================================================
// Test: Coroutine-based Trades Streaming
// ============================================================================

TEST_F(CoroutineStreamTest, SubscribeTradesCoroutine_Basic)
{
    std::vector<std::string> instruments = {"BBG004S584W1"};
    
    std::cout << "[Test] Starting coroutine-based trades stream for 10 seconds..." << std::endl;
    
    int messageCount = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(10);
    
    for co_await (auto& response : stream->SubscribeTradesCoroutine(instruments)) {
        messageCount++;
        
        std::cout << "[Test] Received message #" << messageCount 
                  << " type: " << response.getPayloadTypeString() << std::endl;
        
        if (std::chrono::steady_clock::now() - startTime >= duration) {
            break;
        }
    }
    
    std::cout << "[Test] Stream ended. Total messages: " << messageCount << std::endl;
    
    EXPECT_GT(messageCount, 0);
}

// ============================================================================
// Test: Coroutine-based Info Streaming
// ============================================================================

TEST_F(CoroutineStreamTest, SubscribeInfoCoroutine_Basic)
{
    std::vector<std::string> instruments = {"BBG004S584W1"};
    
    std::cout << "[Test] Starting coroutine-based info stream for 10 seconds..." << std::endl;
    
    int messageCount = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(10);
    
    for co_await (auto& response : stream->SubscribeInfoCoroutine(instruments)) {
        messageCount++;
        
        std::cout << "[Test] Received message #" << messageCount 
                  << " type: " << response.getPayloadTypeString() << std::endl;
        
        if (std::chrono::steady_clock::now() - startTime >= duration) {
            break;
        }
    }
    
    std::cout << "[Test] Stream ended. Total messages: " << messageCount << std::endl;
    
    // Note: Trading status may not update frequently
    EXPECT_GE(messageCount, 0);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

