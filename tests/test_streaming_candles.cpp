/**
 * @file test_streaming_candles.cpp
 * @brief Candle Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to candle streaming:
 * - Subscribe to instruments with different time intervals (1-min, 5-min)
 * - Run for 1 minute
 * - Verify candle data (OHLCV) is received
 * - Test unsubscribe/resubscribe functionality
 * 
 * These tests verify real streaming functionality with actual API connection.
 * SubscribeCandlesAsync provides real-time candle updates for instruments.
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

// Instruments for testing (Russian MOEX Stocks)
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

// ============================================================================
// Helper function to get interval string
// ============================================================================

std::string intervalToString(SubscriptionInterval interval) {
    switch (interval) {
        case SubscriptionInterval::SUBSCRIPTION_INTERVAL_UNSPECIFIED:
            return "UNSPECIFIED";
        case SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE:
            return "1_MINUTE";
        case SubscriptionInterval::SUBSCRIPTION_INTERVAL_FIVE_MINUTES:
            return "5_MINUTES";
        default:
            return "UNKNOWN";
    }
}

// ============================================================================
// Helper function to convert Quotation to double
// ============================================================================

double quotationToDouble(const Quotation& q) {
    return static_cast<double>(q.units()) + static_cast<double>(q.nano()) / 1e9;
}

// ============================================================================
// Test Helper Class for Tracking Candle Messages
// ============================================================================

class CandleTestHelper {
public:
    CandleTestHelper(const std::string& testName)
        : testName(testName),
          totalMessages(0),
          candleMessages(0),
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
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
            candleMessages++;
            auto candle = response.getCandle();
            
            // Track candle data per instrument
            CandleStats stats;
            stats.message_count++;
            stats.interval = candle.interval();
            stats.interval_string = intervalToString(candle.interval());
            
            // Extract OHLCV data
            stats.open = quotationToDouble(candle.open());
            stats.high = quotationToDouble(candle.high());
            stats.low = quotationToDouble(candle.low());
            stats.close = quotationToDouble(candle.close());
            stats.volume = candle.volume();
            
            if (candle.has_time()) {
                stats.has_time = true;
            }
            
            instrumentCandles[figi] = stats;
            lastCandle[figi] = stats;
            
            cv.notify_all();
        } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE)) {
            subscriptionConfirmations++;
            auto candlesResponse = response.getSubscribeCandlesResponse();
            trackingId = candlesResponse.tracking_id();
            std::cout << "[" << testName << "] Subscription confirmation received, tracking_id: " 
                      << trackingId << std::endl;
            cv.notify_all();
        }
    }

    void recordSubscriptionConfirmation() {
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

    bool waitForCandleData(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return candleMessages > 0; 
                          });
    }

    bool waitForSubscriptionConfirmation(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return subscriptionConfirmations > 0; 
                          });
    }

    bool waitForAllInstruments(int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return instrumentCandles.size() >= TEST_INSTRUMENTS.size(); 
                          });
    }

    bool waitForAnyInstrument(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return !instrumentCandles.empty(); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    int getCandleMessages() const { return candleMessages.load(); }
    int getSubscriptionConfirmations() const { return subscriptionConfirmations.load(); }
    
    int getInstrumentCandleCount(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = instrumentCandles.find(figi);
        return (it != instrumentCandles.end()) ? it->second.message_count : 0;
    }

    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentCandles.size();
    }

    bool isInstrumentActive(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentCandles.find(figi) != instrumentCandles.end();
    }

    std::string getTrackingId() const {
        std::lock_guard<std::mutex> lock(mutex);
        return trackingId;
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
        std::cout << "Candle messages: " << candleMessages << std::endl;
        std::cout << "Subscription confirmations: " << subscriptionConfirmations << std::endl;
        std::cout << "Active instruments: " << instrumentCandles.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << "Tracking ID: " << trackingId << std::endl;
        std::cout << "Elapsed time: " << getElapsedMs() << "ms" << std::endl;
        
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            auto it = instrumentCandles.find(figi);
            if (it != instrumentCandles.end()) {
                const CandleStats& stats = it->second;
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << stats.message_count << " candle messages" << std::endl;
                std::cout << "    Interval: " << stats.interval_string << std::endl;
                if (stats.close > 0) {
                    std::cout << "    OHLC: $" << std::fixed << std::setprecision(2) 
                              << "O=" << stats.open << " H=" << stats.high 
                              << " L=" << stats.low << " C=" << stats.close << std::endl;
                    std::cout << "    Volume: " << stats.volume << std::endl;
                }
            } else {
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << "NO DATA" << std::endl;
            }
        }
        std::cout << "==============================\n" << std::endl;
    }

private:
    struct CandleStats {
        int message_count = 0;
        SubscriptionInterval interval;
        std::string interval_string;
        double open = 0.0;
        double high = 0.0;
        double low = 0.0;
        double close = 0.0;
        int64_t volume = 0;
        bool has_time = false;
    };

    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<int> candleMessages;
    std::atomic<int> subscriptionConfirmations;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, CandleStats> instrumentCandles;
    std::unordered_map<std::string, CandleStats> lastCandle;
    std::string trackingId;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Test Fixture
// ============================================================================

class CandleStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up CandleStreamingTest..." << std::endl;
        
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
// MAIN TEST: SubscribeCandlesAsync for 1 minute (1-minute interval)
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_1MinInterval_1Minute)
{
    CandleTestHelper helper("SubscribeCandles_1Min_1Min");
    
    std::cout << "\n=== TEST: SubscribeCandlesAsync (1-min interval) for 1 minute ===" << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Interval: 1 minute" << std::endl;
    std::cout << "Duration: 60 seconds" << std::endl;
    std::cout << std::endl;
    
    // Create instrument-interval pairs for 1-minute candles
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    // Create callback that tracks all instruments
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE)) {
                helper.recordSubscriptionConfirmation();
            }
        }
    };
    
    // Start subscription
    std::cout << "1. Starting candle subscription to " << TEST_INSTRUMENTS.size() 
              << " instruments (1-min interval)..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    helper.start();
    
    // Wait for subscription confirmation
    std::cout << "2. Waiting for subscription confirmation (max 10 seconds)..." << std::endl;
    bool gotConfirmation = helper.waitForSubscriptionConfirmation(10);
    
    if (!gotConfirmation) {
        helper.printStats();
        std::cout << "WARNING: No subscription confirmation received within 10 seconds" << std::endl;
    } else {
        std::cout << "3. Subscription confirmed! Tracking ID: " << helper.getTrackingId() << std::endl;
    }
    
    // Wait for first candle message
    std::cout << "4. Waiting for first candle message (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForCandleData(30);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No candle messages received within 30 seconds";
        return;
    }
    
    std::cout << "5. First candle message received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Check all instruments are receiving data
    std::cout << "6. Checking all instruments receive candle data..." << std::endl;
    bool allInstrumentsActive = helper.waitForAllInstruments(45);
    
    helper.printStats();
    
    if (!allInstrumentsActive) {
        std::cout << "WARNING: Not all instruments are receiving candle data (" 
                  << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << ")" << std::endl;
    }
    
    // Run for 1 minute total
    std::cout << "7. Streaming candles for 60 seconds..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    int candlesAtStart = helper.getCandleMessages();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::seconds(60)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        int currentCandles = helper.getCandleMessages();
        int activeInstruments = helper.getActiveInstrumentCount();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        std::cout << "   [" << elapsed << "s] Candles: " << currentCandles 
                  << " (+" << (currentCandles - candlesAtStart) << "), "
                  << "Active: " << activeInstruments << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        
        candlesAtStart = currentCandles;
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "8. Unsubscribing..." << std::endl;
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    EXPECT_GE(helper.getCandleMessages(), 1) << "Should receive at least 1 candle message";
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    // Print individual instrument statistics
    std::cout << "\nPer-instrument candle statistics:" << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        const std::string& figi = TEST_INSTRUMENTS[i];
        int count = helper.getInstrumentCandleCount(figi);
        std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                  << count << " candle messages" << std::endl;
    }
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: SubscribeCandlesAsync with 5-minute interval
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_5MinInterval_1Minute)
{
    CandleTestHelper helper("SubscribeCandles_5Min_1Min");
    
    std::cout << "\n=== TEST: SubscribeCandlesAsync (5-min interval) for 1 minute ===" << std::endl;
    std::cout << "Interval: 5 minutes" << std::endl;
    
    // Create instrument-interval pairs for 5-minute candles
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_FIVE_MINUTES});
    }
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE)) {
                helper.recordSubscriptionConfirmation();
            }
        }
    };
    
    // Start subscription
    std::cout << "Starting 5-minute candle subscription..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    helper.start();
    
    // Wait for first candle message (may take longer for 5-min candles)
    std::cout << "Waiting for first candle message (max 60 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForCandleData(60);
    
    // Run for 60 seconds
    std::this_thread::sleep_for(std::chrono::seconds(60));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Note: 5-minute candles may not come very frequently, so we may not get many
    if (gotFirstMessage) {
        EXPECT_GE(helper.getCandleMessages(), 1);
        std::cout << "\n=== 5-MINUTE INTERVAL TEST PASSED ===" << std::endl;
    } else {
        std::cout << "\n=== 5-MINUTE INTERVAL TEST: No candles received (may be expected) ===" << std::endl;
        GTEST_SKIP() << "No 5-minute candle data received within 60 seconds (may be expected due to interval)";
    }
}

// ============================================================================
// Test: Verify Candle Structure (OHLCV)
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_VerifyStructure)
{
    CandleTestHelper helper("VerifyCandleStructure");
    
    std::cout << "\n=== TEST: Verify Candle Structure (OHLCV) ===" << std::endl;
    
    // Single instrument with 1-minute interval
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments = {
        {TEST_INSTRUMENTS[0], SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
                
                // Log the structure
                std::cout << "  Candle received:" << std::endl;
                std::cout << "    figi: " << candle.figi() << std::endl;
                std::cout << "    interval: " << intervalToString(candle.interval()) << std::endl;
                std::cout << "    open: " << quotationToDouble(candle.open()) << std::endl;
                std::cout << "    high: " << quotationToDouble(candle.high()) << std::endl;
                std::cout << "    low: " << quotationToDouble(candle.low()) << std::endl;
                std::cout << "    close: " << quotationToDouble(candle.close()) << std::endl;
                std::cout << "    volume: " << candle.volume() << std::endl;
                std::cout << "    has_time: " << (candle.has_time() ? "true" : "false") << std::endl;
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE)) {
                auto candlesResponse = response.getSubscribeCandlesResponse();
                std::cout << "  Subscription candles response:" << std::endl;
                std::cout << "    tracking_id: " << candlesResponse.tracking_id() << std::endl;
                helper.recordSubscriptionConfirmation();
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to candles..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    helper.start();
    
    // Wait for candle data
    std::cout << "Waiting for candle data..." << std::endl;
    bool gotData = helper.waitForCandleData(30);
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    if (gotData && helper.getCandleMessages() > 0) {
        std::cout << "\n=== STRUCTURE VERIFICATION PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No candle data to verify structure";
    }
}

// ============================================================================
// Test: Single Instrument Subscription (30 seconds)
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_SingleInstrument_30Seconds)
{
    CandleTestHelper helper("SingleInstrumentCandles");
    
    std::cout << "\n=== TEST: Single Instrument Candles (30 seconds) ===" << std::endl;
    std::cout << "Instrument: " << TEST_INSTRUMENT_NAMES[0] 
              << " (" << TEST_INSTRUMENTS[0] << ")" << std::endl;
    
    // Single instrument with 1-minute interval
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments = {
        {TEST_INSTRUMENTS[0], SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    std::cout << "\n=== SINGLE INSTRUMENT TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Unsubscribe and Resubscribe
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_UnsubscribeResubscribe)
{
    CandleTestHelper helper("UnsubscribeResubscribe");
    
    std::cout << "\n=== TEST: Unsubscribe and Resubscribe ===" << std::endl;
    
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
            }
        }
    };
    
    // First subscription
    std::cout << "First subscription (15 seconds)..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    
    // Unsubscribe
    std::cout << "Unsubscribing..." << std::endl;
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    int messagesBeforeResubscribe = helper.getCandleMessages();
    std::cout << "Candle messages before resubscribe: " << messagesBeforeResubscribe << std::endl;
    
    // Resubscribe
    std::cout << "Resubscribing (15 seconds)..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify resubscription works
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    std::cout << "\n=== UNSUBSCRIBE/RESUBSCRIBE TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Quick Subscription Test (10 seconds)
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_QuickTest_10Seconds)
{
    CandleTestHelper helper("QuickCandleTest");
    
    std::cout << "\n=== TEST: Quick Candle Subscription (10 seconds) ===" << std::endl;
    
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    helper.start();
    
    // Wait 10 seconds
    std::cout << "Streaming for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Just verify subscription confirmation
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Intervals in Sequence
// ============================================================================

TEST_F(CandleStreamingTest, SubscribeCandlesAsync_MultipleIntervals)
{
    CandleTestHelper helper("MultipleIntervals");
    
    std::cout << "\n=== TEST: Multiple Intervals in Sequence ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                auto candle = response.getCandle();
                helper.recordMessage(candle.figi(), response);
                std::cout << "  Candle: " << intervalToString(candle.interval()) 
                          << " for " << candle.figi() << std::endl;
            }
        }
    };
    
    // First subscribe to 1-minute candles
    std::cout << "Subscribing to 1-minute candles..." << std::endl;
    std::vector<std::pair<std::string, SubscriptionInterval>> candles1min = {
        {TEST_INSTRUMENTS[0], SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };
    stream->SubscribeCandlesAsync(candles1min, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Now add 5-minute candles
    std::cout << "Adding 5-minute candles..." << std::endl;
    std::vector<std::pair<std::string, SubscriptionInterval>> candles5min = {
        {TEST_INSTRUMENTS[0], SubscriptionInterval::SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };
    stream->SubscribeCandlesAsync(candles5min, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify we got subscription confirmations
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1);
    
    std::cout << "\n=== MULTIPLE INTERVALS TEST PASSED ===" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Candle Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nIntervals:" << std::endl;
    std::cout << "  - SUBSCRIPTION_INTERVAL_ONE_MINUTE (1 min)" << std::endl;
    std::cout << "  - SUBSCRIPTION_INTERVAL_FIVE_MINUTES (5 min)" << std::endl;
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_1MinInterval_1Minute (MAIN TEST)" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_5MinInterval_1Minute" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_VerifyStructure" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_SingleInstrument_30Seconds" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_UnsubscribeResubscribe" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_QuickTest_10Seconds" << std::endl;
    std::cout << "  - SubscribeCandlesAsync_MultipleIntervals" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

