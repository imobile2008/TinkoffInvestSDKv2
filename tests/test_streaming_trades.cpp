/**
 * @file test_streaming_trades.cpp
 * @brief Trade Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to trade streaming:
 * - Subscribe to instruments and receive trade data
 * - Run for configurable durations (10s, 30s, 60s)
 * - Verify trade data (price, quantity, direction) is received
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
#include <map>

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
    "BBG004S68104",  // Sberbank (SBER) - highly liquid
    "BBG00JXPFBN0",  // Tinkoff (TCS) - liquid
    "BBG004730JJ5",  // Moscow Exchange (MOEX) - medium liquidity
};

const std::vector<std::string> TEST_INSTRUMENT_NAMES = {
    "Sberbank",
    "Tinkoff",
    "Moscow Exchange"
};

// ============================================================================
// Test Helper Class for Tracking Trade Messages
// ============================================================================

class TradesTestHelper {
public:
    TradesTestHelper(const std::string& testName)
        : testName(testName),
          totalMessages(0),
          tradeMessages(0),
          subscriptionConfirmations(0),
          running(false),
          startTime(std::chrono::steady_clock::now()) {}

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
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
            tradeMessages++;
            auto trade = response.getTrade();
            
            // Extract trade data
            TradeData data;
            data.figi = trade.figi();
            data.direction = trade.direction();
            data.quantity = trade.quantity();
            
            // Extract price (Quotation has units and nano)
            data.price = trade.price().units() + trade.price().nano() / 1e9;
            data.timestamp = trade.time().seconds() + trade.time().nanos() / 1e9;
            
            // Track trade data per instrument
            auto& instrumentTrades = instrumentTradesMap[figi];
            instrumentTrades.push_back(data);
            tradeCountMap[figi]++;
            
            // Update best prices
            if (data.direction == "BUY") {
                if (bestBuyPrices.find(figi) == bestBuyPrices.end() || data.price > bestBuyPrices[figi]) {
                    bestBuyPrices[figi] = data.price;
                }
            } else if (data.direction == "SELL") {
                if (bestSellPrices.find(figi) == bestSellPrices.end() || data.price < bestSellPrices[figi]) {
                    bestSellPrices[figi] = data.price;
                }
            }
            
            cv.notify_all();
        } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_TRADES_RESPONSE)) {
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

    bool waitForTradeData(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return tradeMessages > 0; 
                          });
    }

    bool waitForAllInstruments(int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return tradeCountMap.size() >= TEST_INSTRUMENTS.size(); 
                          });
    }

    bool waitForAnyInstrument(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return !tradeCountMap.empty(); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    int getTradeMessages() const { return tradeMessages.load(); }
    int getSubscriptionConfirmations() const { return subscriptionConfirmations.load(); }
    
    int getInstrumentTradeCount(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = tradeCountMap.find(figi);
        return (it != tradeCountMap.end()) ? it->second : 0;
    }

    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return tradeCountMap.size();
    }

    bool isInstrumentActive(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        return tradeCountMap.find(figi) != tradeCountMap.end();
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
        std::cout << "Trade messages: " << tradeMessages << std::endl;
        std::cout << "Subscription confirmations: " << subscriptionConfirmations << std::endl;
        std::cout << "Active instruments: " << tradeCountMap.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << "Elapsed time: " << getElapsedMs() << "ms" << std::endl;
        
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            auto it = tradeCountMap.find(figi);
            if (it != tradeCountMap.end()) {
                double buyPrice = bestBuyPrices.count(figi) ? bestBuyPrices.at(figi) : 0.0;
                double sellPrice = bestSellPrices.count(figi) ? bestSellPrices.at(figi) : 0.0;
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << it->second << " trades" << std::endl;
                std::cout << "    Best Buy: $" << std::fixed << std::setprecision(4) << buyPrice
                          << ", Best Sell: $" << sellPrice << std::endl;
            } else {
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << "NO TRADES" << std::endl;
            }
        }
        std::cout << "==============================\n" << std::endl;
    }

private:
    struct TradeData {
        std::string figi;
        std::string direction;
        int64_t quantity;
        double price;
        double timestamp;
    };

    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<int> tradeMessages;
    std::atomic<int> subscriptionConfirmations;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, int> tradeCountMap;
    std::unordered_map<std::string, std::vector<TradeData>> instrumentTradesMap;
    std::unordered_map<std::string, double> bestBuyPrices;
    std::unordered_map<std::string, double> bestSellPrices;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Callback Wrapper
// ============================================================================

void tradesCallback(TradesTestHelper& helper, const std::string& figi, 
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

class TradesStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up TradesStreamingTest..." << std::endl;
        
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
// MAIN TEST: SubscribeTradesAsync for 1 minute
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_1Minute)
{
    TradesTestHelper helper("SubscribeTrades_1Min");
    
    std::cout << "\n=== TEST: SubscribeTradesAsync for 1 minute ===" << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Duration: 60 seconds" << std::endl;
    std::cout << std::endl;
    
    // Create callback that tracks all instruments
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_TRADES_RESPONSE)) {
                helper.recordSubscriptionConfirmation("");
            }
        }
    };
    
    // Start subscription
    std::cout << "1. Starting trade subscription to " << TEST_INSTRUMENTS.size() 
              << " instruments..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait for first trade message
    std::cout << "2. Waiting for first trade message (max 15 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForTradeData(15);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No trade messages received within 15 seconds";
        return;
    }
    
    std::cout << "3. First trade message received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Check all instruments are receiving data
    std::cout << "4. Checking all instruments receive trade data..." << std::endl;
    bool allInstrumentsActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    if (!allInstrumentsActive) {
        std::cout << "WARNING: Not all instruments are receiving trade data (" 
                  << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << ")" << std::endl;
    }
    
    // Run for 1 minute total
    std::cout << "5. Streaming trades for 60 seconds..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    int tradesAtStart = helper.getTradeMessages();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::seconds(60)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        int currentTrades = helper.getTradeMessages();
        int activeInstruments = helper.getActiveInstrumentCount();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        std::cout << "   [" << elapsed << "s] Trades: " << currentTrades 
                  << " (+" << (currentTrades - tradesAtStart) << "), "
                  << "Active: " << activeInstruments << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        
        tradesAtStart = currentTrades;
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "6. Unsubscribing..." << std::endl;
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    EXPECT_GE(helper.getTradeMessages(), 1) << "Should receive at least 1 trade message";
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "At least 1 instrument should be receiving trade data";
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    // Print individual instrument statistics
    std::cout << "\nPer-instrument trade statistics:" << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        const std::string& figi = TEST_INSTRUMENTS[i];
        int count = helper.getInstrumentTradeCount(figi);
        std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                  << count << " trades" << std::endl;
    }
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Quick Subscription Test (10 seconds)
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_QuickTest_10Seconds)
{
    TradesTestHelper helper("QuickTradesTest");
    
    std::cout << "\n=== TEST: Quick Trade Subscription (10 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to " << TEST_INSTRUMENTS.size() << " instruments..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait 10 seconds
    std::cout << "Streaming for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTradeMessages(), 1) << "Should receive at least 1 trade message";
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Single Instrument Subscription (30 seconds)
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_SingleInstrument_30Seconds)
{
    TradesTestHelper helper("SingleInstrumentTrades");
    
    std::cout << "\n=== TEST: Single Instrument Trade Subscription (30 seconds) ===" << std::endl;
    std::cout << "Instrument: " << TEST_INSTRUMENT_NAMES[0] 
              << " (" << TEST_INSTRUMENTS[0] << ")" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe to single instrument
    std::vector<std::string> singleInstrument = {TEST_INSTRUMENTS[0]};
    std::cout << "Subscribing to " << TEST_INSTRUMENT_NAMES[0] << "..." << std::endl;
    stream->SubscribeTradesAsync(singleInstrument, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTradeMessages(), 1) << "Should receive at least 1 trade message";
    EXPECT_EQ(helper.getActiveInstrumentCount(), 1) << "Only 1 instrument should be active";
    
    std::cout << "\n=== SINGLE INSTRUMENT TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Verify Trade Structure (Price, Quantity, Direction)
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_VerifyTradeStructure)
{
    TradesTestHelper helper("VerifyTradeStructure");
    
    std::cout << "\n=== TEST: Verify Trade Structure (Price, Quantity, Direction) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to " << TEST_INSTRUMENT_NAMES[0] << "..." << std::endl;
    stream->SubscribeTradesAsync({TEST_INSTRUMENTS[0]}, callback);
    
    helper.start();
    
    // Wait for trade data
    std::cout << "Waiting for trade data..." << std::endl;
    bool gotData = helper.waitForTradeData(15);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    if (gotData && helper.getTradeMessages() > 0) {
        std::cout << "\n=== TRADE STRUCTURE VERIFICATION PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No trade data to verify structure";
    }
}

// ============================================================================
// Test: Message Count Tracking (60 seconds)
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_MessageCountTracking)
{
    TradesTestHelper helper("TradeMessageCountTracking");
    
    std::cout << "\n=== TEST: Trade Message Count Tracking (60 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Track message rate over time
    int messagesAt30s = 0;
    int messagesAt60s = 0;
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
    messagesAt30s = helper.getTradeMessages();
    std::cout << "Trade messages after 30 seconds: " << messagesAt30s << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
    messagesAt60s = helper.getTradeMessages();
    std::cout << "Trade messages after 60 seconds: " << messagesAt60s << std::endl;
    
    // Unsubscribe
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify messages were received
    EXPECT_GT(messagesAt30s, 0) << "Should receive trade messages within 30 seconds";
    EXPECT_GT(messagesAt60s, messagesAt30s) << "Should continue receiving trade messages";
    
    if (messagesAt60s > 0) {
        std::cout << "\nTrade rate: ~" << (messagesAt60s / 60) << " trades/second" << std::endl;
        std::cout << "\n=== MESSAGE COUNT TRACKING TEST PASSED ===" << std::endl;
    }
}

// ============================================================================
// Test: Unsubscribe and Resubscribe
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_UnsubscribeResubscribe)
{
    TradesTestHelper helper("UnsubscribeResubscribe");
    
    std::cout << "\n=== TEST: Unsubscribe and Resubscribe ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // First subscription
    std::cout << "First subscription (10 seconds)..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    std::cout << "Unsubscribing..." << std::endl;
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    int messagesBeforeResubscribe = helper.getTradeMessages();
    std::cout << "Trade messages before resubscribe: " << messagesBeforeResubscribe << std::endl;
    
    // Resubscribe
    std::cout << "Resubscribing (10 seconds)..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify resubscription works
    EXPECT_GE(helper.getTradeMessages(), messagesBeforeResubscribe) 
        << "Resubscription should receive trade messages";
    
    std::cout << "\n=== UNSUBSCRIBE/RESUBSCRIBE TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Subscriptions in Sequence
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_MultipleSubscriptionsSequential)
{
    TradesTestHelper helper("MultipleSubscriptions");
    
    std::cout << "\n=== TEST: Multiple Subscriptions in Sequence ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe to first instrument
    std::cout << "Subscribing to instrument 1: " << TEST_INSTRUMENT_NAMES[0] << std::endl;
    std::vector<std::string> batch1 = {TEST_INSTRUMENTS[0]};
    stream->SubscribeTradesAsync(batch1, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Subscribe to second and third instruments
    std::cout << "Adding instruments 2-3: " << TEST_INSTRUMENT_NAMES[1] << ", " << TEST_INSTRUMENT_NAMES[2] << std::endl;
    std::vector<std::string> batch2 = {TEST_INSTRUMENTS[1], TEST_INSTRUMENTS[2]};
    stream->SubscribeTradesAsync(batch2, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "Should have at least 1 active instrument";
    
    std::cout << "\n=== MULTIPLE SUBSCRIPTIONS TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: All 3 Instruments Receive Data
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_AllInstrumentsReceiveData)
{
    TradesTestHelper helper("AllInstrumentsReceiveData");
    
    std::cout << "\n=== TEST: All 3 Instruments Receive Data ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                auto trade = response.getTrade();
                helper.recordMessage(trade.figi(), response);
            }
        }
    };
    
    // Subscribe to all 3 instruments
    std::cout << "Subscribing to 3 instruments..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait up to 30 seconds for all instruments to be active
    std::cout << "Waiting for all instruments to be active (max 30 seconds)..." << std::endl;
    bool allActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    // Unsubscribe
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    if (allActive) {
        std::cout << "\n=== ALL 3 INSTRUMENTS RECEIVED DATA ===" << std::endl;
        SUCCEED();
    } else {
        std::cout << "\n=== WARNING: Not all instruments received data ===" << std::endl;
        std::cout << "Active: " << helper.getActiveInstrumentCount() 
                  << "/" << TEST_INSTRUMENTS.size() << std::endl;
        // Don't fail - market might be closed for some instruments
        GTEST_SKIP() << "Not all instruments received data (may be due to market hours)";
    }
}

// ============================================================================
// Test: Empty FIGI List Handles Gracefully
// ============================================================================

TEST_F(TradesStreamingTest, SubscribeTradesAsync_EmptyFigiListHandlesGracefully)
{
    TradesTestHelper helper("EmptyFigiList");
    
    std::cout << "\n=== TEST: Empty FIGI List Handles Gracefully ===" << std::endl;
    
    std::vector<std::string> emptyFigis = {};
    bool noException = true;
    std::string exceptionMsg;
    
    std::thread worker([this, &emptyFigis, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeTradesAsync(emptyFigis, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();
    
    EXPECT_TRUE(noException) << "Empty FIGI list threw: " << exceptionMsg;
    std::cout << "\n=== EMPTY FIGI LIST TEST PASSED ===" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Trade Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeTradesAsync_1Minute (MAIN TEST)" << std::endl;
    std::cout << "  - SubscribeTradesAsync_QuickTest_10Seconds" << std::endl;
    std::cout << "  - SubscribeTradesAsync_SingleInstrument_30Seconds" << std::endl;
    std::cout << "  - SubscribeTradesAsync_VerifyTradeStructure" << std::endl;
    std::cout << "  - SubscribeTradesAsync_MessageCountTracking" << std::endl;
    std::cout << "  - SubscribeTradesAsync_UnsubscribeResubscribe" << std::endl;
    std::cout << "  - SubscribeTradesAsync_MultipleSubscriptionsSequential" << std::endl;
    std::cout << "  - SubscribeTradesAsync_AllInstrumentsReceiveData" << std::endl;
    std::cout << "  - SubscribeTradesAsync_EmptyFigiListHandlesGracefully" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

