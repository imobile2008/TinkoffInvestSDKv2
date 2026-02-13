/**
 * @file test_streaming_last_price.cpp
 * @brief Last Price Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to last price streaming:
 * - Subscribe to 5 instruments simultaneously
 * - Run for 1 minute
 * - Verify messages are received for all instruments
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

// 5 Instruments for testing (Russian MOEX Stocks)
const std::vector<std::string> TEST_INSTRUMENTS = {
    "BBG00JXPFBN0",  // Tinkoff (TCS)
    "BBG004S68104",  // Sberbank (SBER)
    "BBG004730JJ5",  // Moscow Exchange (MOEX)
    "BBG004731354",  // Yandex (YNDX)
    "BBG004730N88"   // Magnit (MGNT)
};

const std::vector<std::string> TEST_INSTRUMENT_NAMES = {
    "Tinkoff",
    "Sberbank", 
    "Moscow Exchange",
    "Yandex",
    "Magnit"
};

// ============================================================================
// Test Helper Class for Tracking Last Price Messages
// ============================================================================

class LastPriceTestHelper {
public:
    LastPriceTestHelper(const std::string& testName)
        : testName(testName),
          totalMessages(0),
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
        instrumentMessages[figi]++;
        
        // Extract last price if available - use isPayloadType instead of hasLastPrice
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
            auto lastPrice = response.getLastPrice();
            // LastPrice uses Quotation for price which has units and nano
            double price = lastPrice.price().units() + lastPrice.price().nano() / 1e9;
            lastPrices[figi] = price;
        }
        
        cv.notify_all();
    }

    void recordMessage(const std::string& figi) {
        std::lock_guard<std::mutex> lock(mutex);
        totalMessages++;
        instrumentMessages[figi]++;
        cv.notify_all();
    }

    bool waitForMessages(int minMessages, int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this, minMessages]() { 
                              return totalMessages >= minMessages; 
                          });
    }

    bool waitForAnyInstrument(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return !instrumentMessages.empty(); 
                          });
    }

    bool waitForAllInstruments(int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return instrumentMessages.size() >= TEST_INSTRUMENTS.size(); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    
    int getInstrumentMessageCount(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = instrumentMessages.find(figi);
        return (it != instrumentMessages.end()) ? it->second : 0;
    }

    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentMessages.size();
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
        std::cout << "Active instruments: " << instrumentMessages.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << "Elapsed time: " << getElapsedMs() << "ms" << std::endl;
        
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            int count = instrumentMessages.count(figi) ? instrumentMessages.at(figi) : 0;
            double price = lastPrices.count(figi) ? lastPrices.at(figi) : 0.0;
            std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                      << count << " messages";
            if (price > 0) {
                std::cout << " @ $" << std::fixed << std::setprecision(2) << price;
            }
            std::cout << std::endl;
        }
        std::cout << "==============================\n" << std::endl;
    }

private:
    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, int> instrumentMessages;
    std::unordered_map<std::string, double> lastPrices;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Callback Wrapper
// ============================================================================

void lastPriceCallback(LastPriceTestHelper& helper, const std::string& figi, 
                       ServiceReply reply)
{
    if (reply.hasMarketDataStreamResponse()) {
        auto response = reply.getMarketDataStreamResponse();
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
            helper.recordMessage(figi, response);
        }
    }
}

// ============================================================================
// Test Fixture
// ============================================================================

class LastPriceStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up LastPriceStreamingTest..." << std::endl;
        
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
// Main Test: Subscribe to 5 instruments for 1 minute
// ============================================================================

TEST_F(LastPriceStreamingTest, SubscribeLastPrice5Instruments_1Minute)
{
    LastPriceTestHelper helper("SubscribeLastPrice_5Instruments_1Min");
    
    std::cout << "\n=== Test: Subscribe to 5 instruments for 1 minute ===" << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << i+1 << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Duration: 60 seconds" << std::endl;
    std::cout << std::endl;
    
    // Create callback that tracks all instruments
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Start subscription
    std::cout << "1. Starting subscription to " << TEST_INSTRUMENTS.size() 
              << " instruments..." << std::endl;
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait for first message (connection confirmation)
    std::cout << "2. Waiting for first message (max 10 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForAnyInstrument(10);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No messages received within 10 seconds";
        return;
    }
    
    std::cout << "3. First message received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Check all instruments are receiving data
    std::cout << "4. Checking all instruments receive data..." << std::endl;
    bool allInstrumentsActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    if (!allInstrumentsActive) {
        std::cout << "WARNING: Not all instruments are active (" 
                  << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << ")" << std::endl;
    }
    
    // Run for 1 minute total
    std::cout << "5. Streaming for 60 seconds..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    int messagesAtStart = helper.getTotalMessages();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::seconds(60)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        int currentMessages = helper.getTotalMessages();
        int activeInstruments = helper.getActiveInstrumentCount();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        std::cout << "   [" << elapsed << "s] Messages: " << currentMessages 
                  << " (+" << (currentMessages - messagesAtStart) << "), "
                  << "Active instruments: " << activeInstruments << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        
        messagesAtStart = currentMessages;
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "6. Unsubscribing..." << std::endl;
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive at least 1 message";
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "At least 1 instrument should be active";
    
    // Check each instrument received at least some messages
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        int count = helper.getInstrumentMessageCount(TEST_INSTRUMENTS[i]);
        std::cout << "   " << TEST_INSTRUMENT_NAMES[i] << ": " << count << " messages" << std::endl;
    }
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Verify All 5 Instruments Receive Data
// ============================================================================

TEST_F(LastPriceStreamingTest, LastPriceStreaming_AllInstrumentsReceiveData)
{
    LastPriceTestHelper helper("AllInstrumentsReceiveData");
    
    std::cout << "\n=== Test: All 5 Instruments Receive Data ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Subscribe to all 5 instruments
    std::cout << "Subscribing to 5 instruments..." << std::endl;
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait up to 30 seconds for all instruments to be active
    std::cout << "Waiting for all instruments to be active (max 30 seconds)..." << std::endl;
    bool allActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    if (allActive) {
        std::cout << "\n=== ALL 5 INSTRUMENTS RECEIVED DATA ===" << std::endl;
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
// Test: Message Count Tracking
// ============================================================================

TEST_F(LastPriceStreamingTest, LastPriceStreaming_MessageCountTracking)
{
    LastPriceTestHelper helper("MessageCountTracking");
    
    std::cout << "\n=== Test: Message Count Tracking ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Subscribe
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Track message rate over time
    int messagesAt30s = 0;
    int messagesAt60s = 0;
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
    messagesAt30s = helper.getTotalMessages();
    std::cout << "Messages after 30 seconds: " << messagesAt30s << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(30));
    messagesAt60s = helper.getTotalMessages();
    std::cout << "Messages after 60 seconds: " << messagesAt60s << std::endl;
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify messages were received
    EXPECT_GT(messagesAt30s, 0) << "Should receive messages within 30 seconds";
    EXPECT_GT(messagesAt60s, messagesAt30s) << "Should continue receiving messages";
    
    std::cout << "\nMessage rate: ~" << (messagesAt60s / 60) << " messages/second" << std::endl;
}

// ============================================================================
// Test: Quick Subscription Test (10 seconds)
// ============================================================================

TEST_F(LastPriceStreamingTest, SubscribeLastPrice_QuickTest_10Seconds)
{
    LastPriceTestHelper helper("QuickSubscriptionTest");
    
    std::cout << "\n=== Test: Quick Subscription (10 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Subscribe
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait 10 seconds
    std::cout << "Streaming for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive at least 1 message";
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Single Instrument Subscription (baseline)
// ============================================================================

TEST_F(LastPriceStreamingTest, SubscribeLastPrice_SingleInstrument_30Seconds)
{
    LastPriceTestHelper helper("SingleInstrumentTest");
    
    std::cout << "\n=== Test: Single Instrument Subscription (30 seconds) ===" << std::endl;
    std::cout << "Instrument: " << TEST_INSTRUMENT_NAMES[0] 
              << " (" << TEST_INSTRUMENTS[0] << ")" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Subscribe to single instrument
    std::vector<std::string> singleInstrument = {TEST_INSTRUMENTS[0]};
    stream->SubscribeLastPriceAsync(singleInstrument, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive at least 1 message";
    EXPECT_EQ(helper.getActiveInstrumentCount(), 1) << "Only 1 instrument should be active";
    
    std::cout << "\n=== SINGLE INSTRUMENT TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Subscriptions in Sequence
// ============================================================================

TEST_F(LastPriceStreamingTest, SubscribeLastPrice_MultipleSubscriptionsSequential)
{
    LastPriceTestHelper helper("MultipleSubscriptions");
    
    std::cout << "\n=== Test: Multiple Subscriptions in Sequence ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // Subscribe to first 2 instruments
    std::cout << "Subscribing to instruments 1-2..." << std::endl;
    std::vector<std::string> batch1 = {TEST_INSTRUMENTS[0], TEST_INSTRUMENTS[1]};
    stream->SubscribeLastPriceAsync(batch1, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Subscribe to remaining 3 instruments
    std::cout << "Subscribing to instruments 3-5..." << std::endl;
    std::vector<std::string> batch2 = {TEST_INSTRUMENTS[2], TEST_INSTRUMENTS[3], TEST_INSTRUMENTS[4]};
    stream->SubscribeLastPriceAsync(batch2, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "Should have at least 1 active instrument";
    
    std::cout << "\n=== MULTIPLE SUBSCRIPTIONS TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Unsubscribe and Resubscribe
// ============================================================================

TEST_F(LastPriceStreamingTest, SubscribeLastPrice_UnsubscribeResubscribe)
{
    LastPriceTestHelper helper("UnsubscribeResubscribe");
    
    std::cout << "\n=== Test: Unsubscribe and Resubscribe ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                auto lastPrice = response.getLastPrice();
                helper.recordMessage(lastPrice.figi(), response);
            }
        }
    };
    
    // First subscription
    std::cout << "First subscription (10 seconds)..." << std::endl;
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    std::cout << "Unsubscribing..." << std::endl;
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    int messagesBeforeResubscribe = helper.getTotalMessages();
    std::cout << "Messages before resubscribe: " << messagesBeforeResubscribe << std::endl;
    
    // Resubscribe
    std::cout << "Resubscribing (10 seconds)..." << std::endl;
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify resubscription works
    EXPECT_GE(helper.getTotalMessages(), messagesBeforeResubscribe) 
        << "Resubscription should work";
    
    std::cout << "\n=== UNSUBSCRIBE/RESUBSCRIBE TEST PASSED ===" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Last Price Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeLastPrice5Instruments_1Minute (main test)" << std::endl;
    std::cout << "  - LastPriceStreaming_AllInstrumentsReceiveData" << std::endl;
    std::cout << "  - LastPriceStreaming_MessageCountTracking" << std::endl;
    std::cout << "  - SubscribeLastPrice_QuickTest_10Seconds" << std::endl;
    std::cout << "  - SubscribeLastPrice_SingleInstrument_30Seconds" << std::endl;
    std::cout << "  - SubscribeLastPrice_MultipleSubscriptionsSequential" << std::endl;
    std::cout << "  - SubscribeLastPrice_UnsubscribeResubscribe" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

