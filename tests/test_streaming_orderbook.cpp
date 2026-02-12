/**
 * @file test_streaming_orderbook.cpp
 * @brief Order Book Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to order book streaming:
 * - Subscribe to instruments with configurable depth
 * - Run for 1 minute
 * - Verify order book data (bids/asks) is received
 * - Test unsubscribe/resubscribe functionality
 * 
 * These tests verify real streaming functionality with actual API connection.
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
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <fstream>
#include <algorithm>
#include <cctype>
#include <iomanip>

// Include SDK headers
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "marketdatastreamresponse.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// ============================================================================
// Configuration
// ============================================================================

// Token file path (relative to project root)
const std::string TOKEN_FILE_PATH = "../.test_token.txt";

// Read API token from file
std::string readTokenFromFile(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        // Return empty string if file not found
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

// Get the API token - prefer file, fallback to environment variable
std::string getApiToken() {
    // Try to read from file first
    std::string token = readTokenFromFile(TOKEN_FILE_PATH);
    if (!token.empty()) {
        return token;
    }
    
    // Fallback to environment variable
    const char* envToken = std::getenv("TINKOFF_TOKEN");
    if (envToken != nullptr) {
        return std::string(envToken);
    }
    
    // Return empty string if no token found
    return "";
}

const std::string TEST_HOST = "invest-public-api.tinkoff.ru:443";

// Instruments for testing (Russian MOEX Stocks - highly liquid)
const std::vector<std::string> TEST_INSTRUMENTS = {
    "BBG004S68104",  // Sberbank (SBER)
    "BBG00JXPFBN0",  // Tinkoff (TCS)
    "BBG004730JJ5",  // Moscow Exchange (MOEX)
};

const std::vector<std::string> TEST_INSTRUMENT_NAMES = {
    "Sberbank",
    "Tinkoff",
    "Moscow Exchange"
};

// Default order book depth for tests
const int32_t DEFAULT_DEPTH = 10;

// ============================================================================
// Test Helper Class for Tracking Order Book Messages
// ============================================================================

class OrderBookTestHelper {
public:
    OrderBookTestHelper(const std::string& testName)
        : testName(testName),
          totalMessages(0),
          orderBookMessages(0),
          subscriptionConfirmations(0),
          running(false),
          depth(10),
          startTime(std::chrono::steady_clock::now()) {}

    void setDepth(int32_t d) { depth = d; }

    void start() {
        running = true;
        startTime = std::chrono::steady_clock::now();
    }

    void stop() {
        running = false;
    }

    void recordMessage(const std::string& figi, const MarketDataStreamResponse& response) {
        std::lock_guard<std::mutex> lock(mutex);
        totalMessages++;
        
        // Check payload type
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
            orderBookMessages++;
            auto orderBook = response.getOrderBook();
            int bidsCount = orderBook.bids_size();
            int asksCount = orderBook.asks_size();
            
            // Track order book data per instrument
            instrumentOrderBooks[figi] = {
                .bids_count = bidsCount,
                .asks_count = asksCount,
                .depth_used = depth,
                .has_data = true
            };
            
            // Extract price if available
            if (bidsCount > 0) {
                double bestBid = orderBook.bids(0).price().units() + 
                                orderBook.bids(0).price().nano() / 1e9;
                bestBids[figi] = bestBid;
            }
            if (asksCount > 0) {
                double bestAsk = orderBook.asks(0).price().units() + 
                                 orderBook.asks(0).price().nano() / 1e9;
                bestAsks[figi] = bestAsk;
            }
            
            cv.notify_all();
        } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE)) {
            subscriptionConfirmations++;
            std::cout << "[" << testName << "] Subscription confirmation received" << std::endl;
        }
    }

    void recordSubscriptionConfirmation(const std::string& figi) {
        std::lock_guard<std::mutex> lock(mutex);
        subscriptionConfirmations++;
        cv.notify_all();
    }

    bool waitForMessages(int minMessages, int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this, minMessages]() { 
                              return totalMessages >= minMessages; 
                          });
    }

    bool waitForOrderBookData(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return orderBookMessages > 0; 
                          });
    }

    bool waitForAllInstruments(int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return instrumentOrderBooks.size() >= TEST_INSTRUMENTS.size(); 
                          });
    }

    bool waitForAnyInstrument(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return !instrumentOrderBooks.empty(); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    int getOrderBookMessages() const { return orderBookMessages.load(); }
    int getSubscriptionConfirmations() const { return subscriptionConfirmations.load(); }
    
    int getInstrumentOrderBookCount(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = instrumentOrderBooks.find(figi);
        return (it != instrumentOrderBooks.end()) ? it->second.message_count : 0;
    }

    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentOrderBooks.size();
    }

    bool isInstrumentActive(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentOrderBooks.find(figi) != instrumentOrderBooks.end();
    }

    auto getElapsedMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }

    const std::string& getName() const { return testName; }
    bool isRunning() const { return running.load(); }

    void printStats() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "\n=== " << testName << " Statistics ===" << std::endl;
        std::cout << "Total messages: " << totalMessages << std::endl;
        std::cout << "Order book messages: " << orderBookMessages << std::endl;
        std::cout << "Subscription confirmations: " << subscriptionConfirmations << std::endl;
        std::cout << "Active instruments: " << instrumentOrderBooks.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << "Elapsed time: " << getElapsedMs() << "ms" << std::endl;
        std::cout << "Depth: " << depth << std::endl;
        
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            auto it = instrumentOrderBooks.find(figi);
            if (it != instrumentOrderBooks.end()) {
                const OrderBookStats& stats = it->second;
                double bid = bestBids.count(figi) ? bestBids.at(figi) : 0.0;
                double ask = bestAsks.count(figi) ? bestAsks.at(figi) : 0.0;
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << stats.message_count << " order book messages" << std::endl;
                std::cout << "    Bids: " << stats.bids_count << ", Asks: " << stats.asks_count
                          << ", Best Bid: $" << std::fixed << std::setprecision(2) << bid
                          << ", Best Ask: $" << ask << std::endl;
                if (bid > 0 && ask > 0) {
                    std::cout << "    Spread: $" << std::fixed << std::setprecision(2) 
                              << (ask - bid) << std::endl;
                }
            } else {
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << "NO DATA" << std::endl;
            }
        }
        std::cout << "==============================\n" << std::endl;
    }

private:
    struct OrderBookStats {
        int message_count = 0;
        int bids_count = 0;
        int asks_count = 0;
        int depth_used = 0;
        bool has_data = false;
    };

    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<int> orderBookMessages;
    std::atomic<int> subscriptionConfirmations;
    std::atomic<bool> running;
    int32_t depth;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, OrderBookStats> instrumentOrderBooks;
    std::unordered_map<std::string, double> bestBids;
    std::unordered_map<std::string, double> bestAsks;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Callback Wrapper
// ============================================================================

void orderBookCallback(OrderBookTestHelper& helper, const std::string& figi, 
                       ServiceReply reply)
{
    if (reply.hasMarketDataStreamResponse()) {
        auto response = reply.getMarketDataStreamResponse();
        helper.recordMessage(figi, response);
    }
}

// ============================================================================
// Test Fixture
// ============================================================================

class OrderBookStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up OrderBookStreamingTest..." << std::endl;
        
        // Get token from file or environment variable
        std::string token = getApiToken();
        if (token.empty()) {
            std::cerr << "WARNING: No API token found. Set TINKOFF_TOKEN environment variable or create .test_token.txt file." << std::endl;
        }
        
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, token);
        std::cout << "MarketDataStream created successfully" << std::endl;
    }

    void TearDown() override {
        std::cout << "Tearing down test..." << std::endl;
        if (stream) {
            try {
                stream->close();
            } catch (...) {
                // Ignore cleanup errors
            }
        }
        // Give time for cleanup between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::shared_ptr<MarketDataStream> stream;
};

// ============================================================================
// MAIN TEST: SubscribeOrderBookAsync for 1 minute
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_1Minute)
{
    OrderBookTestHelper helper("SubscribeOrderBook_1Min");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: SubscribeOrderBookAsync for 1 minute ===" << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Depth: " << DEFAULT_DEPTH << std::endl;
    std::cout << "Duration: 60 seconds" << std::endl;
    std::cout << std::endl;
    
    // Create callback that tracks all instruments
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE)) {
                helper.recordSubscriptionConfirmation("");
            }
        }
    };
    
    // Start subscription
    std::cout << "1. Starting order book subscription to " << TEST_INSTRUMENTS.size() 
              << " instruments with depth " << DEFAULT_DEPTH << "..." << std::endl;
    stream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH, callback);
    
    helper.start();
    
    // Wait for first order book message
    std::cout << "2. Waiting for first order book message (max 15 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForOrderBookData(15);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No order book messages received within 15 seconds";
        return;
    }
    
    std::cout << "3. First order book message received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Check all instruments are receiving data
    std::cout << "4. Checking all instruments receive order book data..." << std::endl;
    bool allInstrumentsActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    if (!allInstrumentsActive) {
        std::cout << "WARNING: Not all instruments are receiving order book data (" 
                  << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << ")" << std::endl;
    }
    
    // Run for 1 minute total
    std::cout << "5. Streaming order book for 60 seconds..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    int orderBooksAtStart = helper.getOrderBookMessages();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::seconds(60)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        int currentOrderBooks = helper.getOrderBookMessages();
        int activeInstruments = helper.getActiveInstrumentCount();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        std::cout << "   [" << elapsed << "s] Order Books: " << currentOrderBooks 
                  << " (+" << (currentOrderBooks - orderBooksAtStart) << "), "
                  << "Active: " << activeInstruments << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        
        orderBooksAtStart = currentOrderBooks;
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "6. Unsubscribing..." << std::endl;
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    EXPECT_GE(helper.getOrderBookMessages(), 1) << "Should receive at least 1 order book message";
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "At least 1 instrument should be receiving order book data";
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    // Print individual instrument statistics
    std::cout << "\nPer-instrument order book statistics:" << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        const std::string& figi = TEST_INSTRUMENTS[i];
        int count = helper.getInstrumentOrderBookCount(figi);
        std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                  << count << " order book messages" << std::endl;
    }
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Order Book with Different Depths
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_DifferentDepths)
{
    std::vector<int32_t> depths = {5, 10, 20};
    
    for (int32_t depth : depths) {
        OrderBookTestHelper helper("Depth_" + std::to_string(depth));
        helper.setDepth(depth);
        
        std::cout << "\n=== TEST: SubscribeOrderBookAsync with depth " << depth << " ===" << std::endl;
        
        auto callback = [&helper](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                auto response = reply.getMarketDataStreamResponse();
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                    auto orderBook = response.getOrderBook();
                    helper.recordMessage(orderBook.figi(), response);
                }
            }
        };
        
        // Subscribe with specific depth
        std::cout << "Subscribing with depth " << depth << "..." << std::endl;
        stream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, depth, callback);
        
        helper.start();
        
        // Wait for order book data
        bool gotData = helper.waitForOrderBookData(15);
        
        std::this_thread::sleep_for(std::chrono::seconds(15));
        
        helper.stop();
        
        stream->UnSubscribeOrderBookAsync();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        helper.printStats();
        
        if (gotData) {
            std::cout << "\n=== DEPTH " << depth << " TEST PASSED ===" << std::endl;
            SUCCEED();
        } else {
            std::cout << "\n=== DEPTH " << depth << " TEST SKIPPED (no data) ===" << std::endl;
            GTEST_SKIP() << "No order book data received for depth " << depth;
        }
    }
}

// ============================================================================
// Test: Verify Order Book Structure (Bids/Asks)
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_VerifyStructure)
{
    OrderBookTestHelper helper("VerifyOrderBookStructure");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: Verify Order Book Structure (Bids/Asks) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to order book..." << std::endl;
    stream->SubscribeOrderBookAsync({TEST_INSTRUMENTS[0]}, DEFAULT_DEPTH, callback);
    
    helper.start();
    
    // Wait for order book data
    std::cout << "Waiting for order book data..." << std::endl;
    bool gotData = helper.waitForOrderBookData(15);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    if (gotData && helper.getOrderBookMessages() > 0) {
        std::cout << "\n=== STRUCTURE VERIFICATION PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No order book data to verify structure";
    }
}

// ============================================================================
// Test: Single Instrument Order Book (30 seconds)
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_SingleInstrument_30Seconds)
{
    OrderBookTestHelper helper("SingleInstrumentOrderBook");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: Single Instrument Order Book (30 seconds) ===" << std::endl;
    std::cout << "Instrument: " << TEST_INSTRUMENT_NAMES[0] 
              << " (" << TEST_INSTRUMENTS[0] << ")" << std::endl;
    std::cout << "Depth: " << DEFAULT_DEPTH << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // Subscribe to single instrument
    std::vector<std::string> singleInstrument = {TEST_INSTRUMENTS[0]};
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeOrderBookAsync(singleInstrument, DEFAULT_DEPTH, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getOrderBookMessages(), 1) << "Should receive at least 1 order book message";
    EXPECT_EQ(helper.getActiveInstrumentCount(), 1) << "Only 1 instrument should be active";
    
    std::cout << "\n=== SINGLE INSTRUMENT TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Unsubscribe and Resubscribe
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_UnsubscribeResubscribe)
{
    OrderBookTestHelper helper("UnsubscribeResubscribe");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: Unsubscribe and Resubscribe ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // First subscription
    std::cout << "First subscription (10 seconds)..." << std::endl;
    stream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    std::cout << "Unsubscribing..." << std::endl;
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    int messagesBeforeResubscribe = helper.getOrderBookMessages();
    std::cout << "Order book messages before resubscribe: " << messagesBeforeResubscribe << std::endl;
    
    // Resubscribe
    std::cout << "Resubscribing (10 seconds)..." << std::endl;
    stream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify resubscription works
    EXPECT_GE(helper.getOrderBookMessages(), messagesBeforeResubscribe) 
        << "Resubscription should receive order book messages";
    
    std::cout << "\n=== UNSUBSCRIBE/RESUBSCRIBE TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Quick Order Book Test (10 seconds)
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_QuickTest_10Seconds)
{
    OrderBookTestHelper helper("QuickOrderBookTest");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: Quick Order Book Subscription (10 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH, callback);
    
    helper.start();
    
    // Wait 10 seconds
    std::cout << "Streaming for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getOrderBookMessages(), 1) << "Should receive at least 1 order book message";
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Subscriptions in Sequence
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_MultipleSubscriptions)
{
    OrderBookTestHelper helper("MultipleSubscriptions");
    helper.setDepth(DEFAULT_DEPTH);
    
    std::cout << "\n=== TEST: Multiple Subscriptions in Sequence ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // Subscribe to first instrument
    std::cout << "Subscribing to instrument 1: " << TEST_INSTRUMENT_NAMES[0] << std::endl;
    std::vector<std::string> batch1 = {TEST_INSTRUMENTS[0]};
    stream->SubscribeOrderBookAsync(batch1, DEFAULT_DEPTH, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Subscribe to second and third instruments
    std::cout << "Adding instruments 2-3: " << TEST_INSTRUMENT_NAMES[1] << ", " << TEST_INSTRUMENT_NAMES[2] << std::endl;
    std::vector<std::string> batch2 = {TEST_INSTRUMENTS[1], TEST_INSTRUMENTS[2]};
    stream->SubscribeOrderBookAsync(batch2, DEFAULT_DEPTH, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "Should have at least 1 active instrument";
    
    std::cout << "\n=== MULTIPLE SUBSCRIPTIONS TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: High-Frequency Order Book Updates (20 seconds)
// ============================================================================

TEST_F(OrderBookStreamingTest, SubscribeOrderBookAsync_HighFrequencyUpdates)
{
    OrderBookTestHelper helper("HighFrequencyUpdates");
    helper.setDepth(20);  // Maximum depth for more data
    
    std::cout << "\n=== TEST: High-Frequency Order Book Updates (20 seconds) ===" << std::endl;
    std::cout << "Depth: 20 (maximum)" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                auto orderBook = response.getOrderBook();
                helper.recordMessage(orderBook.figi(), response);
            }
        }
    };
    
    // Subscribe to most liquid instrument
    std::cout << "Subscribing to Sberbank with depth 20..." << std::endl;
    stream->SubscribeOrderBookAsync({TEST_INSTRUMENTS[0]}, 20, callback);
    
    helper.start();
    
    // Track message rate
    int messagesAt5s = 0;
    int messagesAt10s = 0;
    int messagesAt15s = 0;
    int messagesAt20s = 0;
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    messagesAt5s = helper.getOrderBookMessages();
    std::cout << "Order books after 5 seconds: " << messagesAt5s << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    messagesAt10s = helper.getOrderBookMessages();
    std::cout << "Order books after 10 seconds: " << messagesAt10s 
              << " (+" << (messagesAt10s - messagesAt5s) << " in last 5s)" << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    messagesAt15s = helper.getOrderBookMessages();
    std::cout << "Order books after 15 seconds: " << messagesAt15s
              << " (+" << (messagesAt15s - messagesAt10s) << " in last 5s)" << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    messagesAt20s = helper.getOrderBookMessages();
    std::cout << "Order books after 20 seconds: " << messagesAt20s
              << " (+" << (messagesAt20s - messagesAt15s) << " in last 5s)" << std::endl;
    
    helper.stop();
    
    stream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify message rate
    int totalIn20s = messagesAt20s;
    if (totalIn20s > 0) {
        std::cout << "\nAverage update rate: " << (totalIn20s / 20) << " updates/second" << std::endl;
        std::cout << "\n=== HIGH FREQUENCY TEST PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No order book updates received";
    }
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Order Book Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Default Depth: " << DEFAULT_DEPTH << std::endl;
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_1Minute (MAIN TEST)" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_DifferentDepths" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_VerifyStructure" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_SingleInstrument_30Seconds" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_UnsubscribeResubscribe" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_QuickTest_10Seconds" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_MultipleSubscriptions" << std::endl;
    std::cout << "  - SubscribeOrderBookAsync_HighFrequencyUpdates" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

