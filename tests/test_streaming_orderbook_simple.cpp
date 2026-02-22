/**
 * @file test_streaming_orderbook_simple.cpp
 * @brief Simple OrderBook Streaming Test for TinkoffInvestSDK
 * 
 * This file demonstrates how to subscribe to orderbook data streaming.
 * 
 * Usage:
 *   1. Set TINKOFF_TOKEN environment variable or create .test_token.txt file
 *   2. Build and run: ./test_streaming_orderbook_simple
 *   3. The test will subscribe to orderbook for 30 seconds and display messages
 */

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <cctype>

// Include SDK headers
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "marketdatastreamresponse.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// ============================================================================
// Configuration
// ============================================================================

const std::string TOKEN_FILE_PATH = "../.test_token.txt";

// Read API token from file
std::string readTokenFromFile(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        return "";
    }
    
    std::string token;
    std::getline(file, token);
    
    // Remove whitespace and comments
    token.erase(std::remove_if(token.begin(), token.end(), 
                               [](unsigned char c) { return std::isspace(c) || c == '#'; }),
                token.end());
    
    file.close();
    return token;
}

// Get the API token
std::string getApiToken() {
    std::string token = readTokenFromFile(TOKEN_FILE_PATH);
    if (!token.empty()) {
        return token;
    }
    
    const char* envToken = std::getenv("TINKOFF_TOKEN");
    if (envToken != nullptr) {
        return std::string(envToken);
    }
    
    return "";
}

const std::string TEST_HOST = "invest-public-api.tinkoff.ru:443";

// Test instrument (Magnit - as shown in your log)
const std::string TEST_FIGI = "BBG004730N88";

// Order book depth
const int32_t ORDERBOOK_DEPTH = 10;

// Test duration in seconds
const int TEST_DURATION_SECONDS = 30;

// ============================================================================
// OrderBook Message Handler
// ============================================================================

class OrderBookMessageHandler {
public:
    OrderBookMessageHandler() 
        : orderBookCount(0), running(false), startTime(std::chrono::steady_clock::now()) {}

    void start() {
        running = true;
        startTime = std::chrono::steady_clock::now();
    }

    void stop() {
        running = false;
    }

    void handleMessage(const MarketDataStreamResponse& response) {
        std::lock_guard<std::mutex> lock(mutex);
        orderBookCount++;
        
        // Get orderbook data
        auto orderBook = response.getOrderBook();
        std::string figi = orderBook.figi();
        int bidsCount = orderBook.bids_size();
        int asksCount = orderBook.asks_size();
        
        // Get best bid/ask prices
        double bestBid = 0.0;
        double bestAsk = 0.0;
        
        if (bidsCount > 0) {
            bestBid = orderBook.bids(0).price().units() + 
                     orderBook.bids(0).price().nano() / 1e9;
        }
        if (asksCount > 0) {
            bestAsk = orderBook.asks(0).price().units() + 
                     orderBook.asks(0).price().nano() / 1e9;
        }
        
        // Calculate time since start
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        // Print orderbook message
        std::cout << "[" << std::setfill('0') << std::setw(4) << elapsed << "s] "
                  << "MSG #" << orderBookCount << " | "
                  << "Type: ORDERBOOK | "
                  << "FIGI: " << figi << " | "
                  << "Bids: " << bidsCount << ", Asks: " << asksCount << " | "
                  << "Best Bid: " << std::fixed << std::setprecision(2) << bestBid 
                  << ", Best Ask: " << bestAsk;
        
        if (bestBid > 0 && bestAsk > 0) {
            double spread = bestAsk - bestBid;
            std::cout << ", Spread: " << std::fixed << std::setprecision(2) << spread;
        }
        
        std::cout << std::endl;
        
        cv.notify_all();
    }

    bool waitForOrderBook(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { return orderBookCount > 0; });
    }

    int getOrderBookCount() const { return orderBookCount.load(); }

private:
    std::atomic<int> orderBookCount;
    std::atomic<bool> running;
    std::chrono::steady_clock::time_point startTime;
    mutable std::mutex mutex;
    std::condition_variable cv;
};

// ============================================================================
// Test Fixture
// ============================================================================

class OrderBookStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "======================================================" << std::endl;
        std::cout << "Setting up OrderBookStreamingTest..." << std::endl;
        std::cout << "======================================================" << std::endl;
        
        std::string token = getApiToken();
        if (token.empty()) {
            std::cerr << "WARNING: No API token found!" << std::endl;
            std::cerr << "Set TINKOFF_TOKEN env var or create .test_token.txt" << std::endl;
        } else {
            std::cout << "API token found (length: " << token.length() << ")" << std::endl;
        }
        
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, token);
        std::cout << "MarketDataStream created" << std::endl;
        std::cout << "Test FIGI: " << TEST_FIGI << " (Magnit)" << std::endl;
        std::cout << "Order Book Depth: " << ORDERBOOK_DEPTH << std::endl;
        std::cout << "Duration: " << TEST_DURATION_SECONDS << " seconds" << std::endl;
    }

    void TearDown() override {
        if (stream) {
            stream->close();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::shared_ptr<MarketDataStream> stream;
};

// ============================================================================
// Main Test: Subscribe to OrderBook
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_SimpleTest)
{
    OrderBookMessageHandler handler;
    
    std::cout << "\n####################################################" << std::endl;
    std::cout << "#          ORDERBOOK STREAMING TEST                 #" << std::endl;
    std::cout << "####################################################" << std::endl;
    std::cout << "\nThis test will:" << std::endl;
    std::cout << "  1. Subscribe to orderbook for " << TEST_FIGI << std::endl;
    std::cout << "  2. Wait " << TEST_DURATION_SECONDS << " seconds for orderbook updates" << std::endl;
    std::cout << "  3. Display all orderbook messages" << std::endl;
    std::cout << "\n";
    
    // Create callback that handles orderbook messages
    auto callback = [&handler](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            
            // Check for subscription confirmation
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE)) {
                std::cout << "[SUBSCRIPTION] Orderbook subscription confirmed!" << std::endl;
                return;
            }
            
            // Check for ORDERBOOK payload
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                handler.handleMessage(response);
                return;
            }
            
            // Check for other payload types
            std::string payloadType = response.getPayloadTypeString();
            if (payloadType != "UNKNOWN") {
                std::cout << "[OTHER] Received: " << payloadType << std::endl;
            }
        }
    };
    
    // Subscribe to orderbook
    std::cout << ">>> Subscribing to ORDERBOOK..." << std::endl;
    std::vector<std::string> instruments = {TEST_FIGI};
    stream->SubscribeOrderBookAsync(instruments, ORDERBOOK_DEPTH, callback);
    
    handler.start();
    
    // Wait for first orderbook message
    std::cout << ">>> Waiting for first ORDERBOOK message (max 15 seconds)..." << std::endl;
    bool gotFirstMessage = handler.waitForOrderBook(15);
    
    if (!gotFirstMessage) {
        std::cout << "\n[ERROR] No ORDERBOOK messages received within 15 seconds!" << std::endl;
        std::cout << "Possible reasons:" << std::endl;
        std::cout << "  - Market is closed" << std::endl;
        std::cout << "  - Invalid FIGI" << std::endl;
        std::cout << "  - Network issues" << std::endl;
        GTEST_FAIL() << "No orderbook messages received";
        return;
    }
    
    std::cout << ">>> First ORDERBOOK message received!" << std::endl;
    std::cout << ">>> Streaming for " << TEST_DURATION_SECONDS << " seconds..." << std::endl;
    std::cout << "\n";
    
    // Wait for test duration
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(TEST_DURATION_SECONDS);
    
    while (std::chrono::steady_clock::now() < endTime) {
        auto remaining = std::chrono::duration_cast<std::chrono::seconds>(
            endTime - std::chrono::steady_clock::now()).count();
        
        if (remaining % 10 == 0 && remaining != TEST_DURATION_SECONDS) {
            int elapsed = TEST_DURATION_SECONDS - remaining;
            std::cout << ">>> [" << elapsed << "s / " << TEST_DURATION_SECONDS << "s] "
                      << "OrderBooks: " << handler.getOrderBookCount() 
                      << " | " << remaining << "s remaining..." << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    handler.stop();
    
    // Unsubscribe
    std::cout << "\n>>> Unsubscribing..." << std::endl;
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Print summary
    int totalOrderBooks = handler.getOrderBookCount();
    
    std::cout << "\n########################################" << std::endl;
    std::cout << "#          TEST RESULTS                 #" << std::endl;
    std::cout << "########################################" << std::endl;
    std::cout << "Total ORDERBOOK messages: " << totalOrderBooks << std::endl;
    std::cout << "Test duration: " << TEST_DURATION_SECONDS << " seconds" << std::endl;
    
    if (totalOrderBooks > 0) {
        double msgsPerSec = static_cast<double>(totalOrderBooks) / TEST_DURATION_SECONDS;
        std::cout << "Average rate: " << std::fixed << std::setprecision(2) 
                  << msgsPerSec << " messages/second" << std::endl;
    }
    std::cout << "########################################" << std::endl;
    
    // Verify
    EXPECT_GE(totalOrderBooks, 1) << "Should receive at least 1 orderbook message";
    
    std::cout << "\n[SUCCESS] Test completed!" << std::endl;
}

// ============================================================================
// Test: Subscribe to Multiple Instruments OrderBook
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_MultipleInstruments)
{
    OrderBookMessageHandler handler;
    
    // Multiple instruments
    std::vector<std::string> instruments = {
        "BBG004730N88",  // Magnit
        "BBG004S68104",  // Sberbank
        "BBG00JXPFBN0"   // Tinkoff
    };
    
    std::vector<std::string> names = {"Magnit", "Sberbank", "Tinkoff"};
    
    std::cout << "\n####################################################" << std::endl;
    std::cout << "#     MULTIPLE INSTRUMENTS ORDERBOOK TEST           #" << std::endl;
    std::cout << "####################################################" << std::endl;
    std::cout << "Instruments:" << std::endl;
    for (size_t i = 0; i < instruments.size(); i++) {
        std::cout << "  " << (i+1) << ". " << names[i] << " (" << instruments[i] << ")" << std::endl;
    }
    std::cout << "Duration: " << TEST_DURATION_SECONDS << " seconds" << std::endl;
    
    // Create callback
    auto callback = [&handler](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE)) {
                std::cout << "[SUBSCRIPTION] Orderbook subscription confirmed!" << std::endl;
                return;
            }
            
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                handler.handleMessage(response);
            }
        }
    };
    
    // Subscribe
    std::cout << "\n>>> Subscribing to orderbook for " << instruments.size() << " instruments..." << std::endl;
    stream->SubscribeOrderBookAsync(instruments, ORDERBOOK_DEPTH, callback);
    
    handler.start();
    
    // Wait for first message
    bool gotFirst = handler.waitForOrderBook(15);
    if (!gotFirst) {
        GTEST_FAIL() << "No orderbook messages received";
        return;
    }
    
    std::cout << ">>> First orderbook received! Streaming for " << TEST_DURATION_SECONDS << "s..." << std::endl;
    
    // Wait
    std::this_thread::sleep_for(std::chrono::seconds(TEST_DURATION_SECONDS));
    
    handler.stop();
    
    // Unsubscribe
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    std::cout << "\n[RESULT] Received " << handler.getOrderBookCount() << " orderbook messages" << std::endl;
    
    EXPECT_GE(handler.getOrderBookCount(), 1);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK OrderBook Streaming Test" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nThis test demonstrates ORDERBOOK subscription." << std::endl;
    std::cout << "Unlike CANDLE subscription, ORDERBOOK provides" << std::endl;
    std::cout << "bid/ask price levels (market depth)." << std::endl;
    std::cout << "\nTo run:" << std::endl;
    std::cout << "  1. Set TINKOFF_TOKEN or create .test_token.txt" << std::endl;
    std::cout << "  2. Build and run this test" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

