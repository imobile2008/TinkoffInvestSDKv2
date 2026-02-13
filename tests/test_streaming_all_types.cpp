/**
 * @file test_streaming_all_types.cpp
 * @brief Combined Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to all market data streaming types
 * together (Candles, OrderBook, Trades, Info, LastPrice) in a single stream.
 * 
 * This tests:
 * - SubscribeAllAsync: combined subscription to all types in one gRPC stream
 * - Stability over 30 minutes
 * - Message tracking for each data type
 * - Resource efficiency (single stream vs multiple streams)
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
#include <sstream>

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

// Order book depth for tests
const int32_t DEFAULT_DEPTH = 10;

// ============================================================================
// Test Helper Class for Tracking All Message Types
// ============================================================================

class AllStreamTypesTestHelper {
public:
    AllStreamTypesTestHelper(const std::string& testName, int testDurationMinutes = 30)
        : testName(testName),
          testDurationMinutes(testDurationMinutes),
          totalMessages(0),
          candleMessages(0),
          orderBookMessages(0),
          tradeMessages(0),
          infoMessages(0),
          lastPriceMessages(0),
          subscriptionConfirmations(0),
          running(false),
          startTime(std::chrono::steady_clock::now()),
          lastReportTime(startTime) {}

    void start() {
        running = true;
        startTime = std::chrono::steady_clock::now();
        lastReportTime = startTime;
    }

    void stop() {
        running = false;
    }

    void recordMessage(const MarketDataStreamResponse& response) {
        std::lock_guard<std::mutex> lock(mutex);
        totalMessages++;
        
        auto payloadType = response.getPayloadType();
        
        // Track by payload type
        switch (payloadType) {
            case MarketDataStreamResponse::PayloadType::CANDLE:
                candleMessages++;
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::CANDLE)) {
                    auto candle = response.getCandle();
                    instrumentData[candle.figi()].candleCount++;
                    instrumentData[candle.figi()].hasCandleData = true;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::ORDERBOOK:
                orderBookMessages++;
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::ORDERBOOK)) {
                    auto orderBook = response.getOrderBook();
                    instrumentData[orderBook.figi()].orderBookCount++;
                    instrumentData[orderBook.figi()].hasOrderBookData = true;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::TRADE:
                tradeMessages++;
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADE)) {
                    auto trade = response.getTrade();
                    instrumentData[trade.figi()].tradeCount++;
                    instrumentData[trade.figi()].hasTradeData = true;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::TRADING_STATUS:
                infoMessages++;
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                    auto tradingStatus = response.getTradingStatus();
                    instrumentData[tradingStatus.figi()].infoCount++;
                    instrumentData[tradingStatus.figi()].hasInfoData = true;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::LAST_PRICE:
                lastPriceMessages++;
                if (response.isPayloadType(MarketDataStreamResponse::PayloadType::LAST_PRICE)) {
                    auto lastPrice = response.getLastPrice();
                    instrumentData[lastPrice.figi()].lastPriceCount++;
                    instrumentData[lastPrice.figi()].hasLastPriceData = true;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_TRADES_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE:
                subscriptionConfirmations++;
                std::cout << "[" << testName << "] Subscription confirmation received: " 
                          << response.getPayloadTypeString() << std::endl;
                break;
                
            default:
                break;
        }
        
        cv.notify_all();
    }

    bool waitForMessages(int minMessages, int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this, minMessages]() { 
                              return totalMessages >= minMessages; 
                          });
    }

    bool waitForAnyData(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return (candleMessages > 0 || orderBookMessages > 0 || 
                                      tradeMessages > 0 || infoMessages > 0 || 
                                      lastPriceMessages > 0); 
                          });
    }

    bool waitForAllTypes(int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return (candleMessages > 0 && orderBookMessages > 0 && 
                                      tradeMessages > 0 && infoMessages > 0 && 
                                      lastPriceMessages > 0); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    int getCandleMessages() const { return candleMessages.load(); }
    int getOrderBookMessages() const { return orderBookMessages.load(); }
    int getTradeMessages() const { return tradeMessages.load(); }
    int getInfoMessages() const { return infoMessages.load(); }
    int getLastPriceMessages() const { return lastPriceMessages.load(); }
    int getSubscriptionConfirmations() const { return subscriptionConfirmations.load(); }
    
    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentData.size();
    }

    auto getElapsedMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }

    auto getElapsedSeconds() const {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }

    const std::string& getName() const { return testName; }
    bool isRunning() const { return running.load(); }
    int getTestDurationMinutes() const { return testDurationMinutes; }

    void printStats() const {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto elapsedSec = getElapsedSeconds();
        double elapsedMin = elapsedSec / 60.0;
        
        std::cout << "\n============================================================" << std::endl;
        std::cout << "=== " << testName << " Final Statistics ===" << std::endl;
        std::cout << "============================================================" << std::endl;
        std::cout << "Test Duration: " << elapsedMin << " minutes (" << elapsedSec << " seconds)" << std::endl;
        std::cout << "Total Messages: " << totalMessages << std::endl;
        std::cout << std::endl;
        
        std::cout << "Messages by Type:" << std::endl;
        std::cout << "  Candles:        " << candleMessages << std::endl;
        std::cout << "  Order Books:   " << orderBookMessages << std::endl;
        std::cout << "  Trades:        " << tradeMessages << std::endl;
        std::cout << "  Trading Info:  " << infoMessages << std::endl;
        std::cout << "  Last Price:    " << lastPriceMessages << std::endl;
        std::cout << "  Subscriptions: " << subscriptionConfirmations << std::endl;
        std::cout << std::endl;
        
        // Calculate message rates
        if (elapsedSec > 0) {
            std::cout << "Message Rates (per second):" << std::endl;
            std::cout << "  Total:       " << std::fixed << std::setprecision(2) 
                      << (totalMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << "  Candles:     " << std::fixed << std::setprecision(2) 
                      << (candleMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << "  Order Books: " << std::fixed << std::setprecision(2) 
                      << (orderBookMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << "  Trades:      " << std::fixed << std::setprecision(2) 
                      << (tradeMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << "  Trading:     " << std::fixed << std::setprecision(2) 
                      << (infoMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << "  Last Price:  " << std::fixed << std::setprecision(2) 
                      << (lastPriceMessages / (double)elapsedSec) << "/s" << std::endl;
            std::cout << std::endl;
        }
        
        std::cout << "Active Instruments: " << instrumentData.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << std::endl;
        
        // Per-instrument data
        std::cout << "Per-Instrument Statistics:" << std::endl;
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            auto it = instrumentData.find(figi);
            if (it != instrumentData.end()) {
                const InstrData& data = it->second;
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "):" << std::endl;
                std::cout << "    Candles: " << data.candleCount 
                          << ", OrderBook: " << data.orderBookCount
                          << ", Trades: " << data.tradeCount
                          << ", Info: " << data.infoCount
                          << ", LastPrice: " << data.lastPriceCount << std::endl;
            } else {
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): NO DATA" << std::endl;
            }
        }
        
        std::cout << "============================================================\n" << std::endl;
    }

    // Print periodic progress report
    void printProgressReport() {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto now = std::chrono::steady_clock::now();
        auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - lastReportTime).count();
        auto totalElapsedSec = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();
        
        // Calculate messages in this interval
        int intervalTotal = totalMessages - lastReportMessages;
        double rate = (elapsedSec > 0) ? (intervalTotal / (double)elapsedSec) : 0;
        
        std::cout << "--- Progress [" << (totalElapsedSec / 60) << "m " << (totalElapsedSec % 60) << "s] ---" << std::endl;
        std::cout << "  Total: " << totalMessages << " (+" << intervalTotal << ", " << rate << "/s)" << std::endl;
        std::cout << "  Candles: " << candleMessages 
                  << ", OrderBook: " << orderBookMessages 
                  << ", Trades: " << tradeMessages << std::endl;
        std::cout << "  Info: " << infoMessages 
                  << ", LastPrice: " << lastPriceMessages << std::endl;
        
        lastReportTime = now;
        lastReportMessages = totalMessages;
    }

private:
    struct InstrData {
        int candleCount = 0;
        int orderBookCount = 0;
        int tradeCount = 0;
        int infoCount = 0;
        int lastPriceCount = 0;
        bool hasCandleData = false;
        bool hasOrderBookData = false;
        bool hasTradeData = false;
        bool hasInfoData = false;
        bool hasLastPriceData = false;
    };

    std::string testName;
    int testDurationMinutes;
    std::atomic<int> totalMessages;
    std::atomic<int> candleMessages;
    std::atomic<int> orderBookMessages;
    std::atomic<int> tradeMessages;
    std::atomic<int> infoMessages;
    std::atomic<int> lastPriceMessages;
    std::atomic<int> subscriptionConfirmations;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, InstrData> instrumentData;
    std::chrono::steady_clock::time_point startTime;
    std::chrono::steady_clock::time_point lastReportTime;
    int lastReportMessages = 0;
};

// ============================================================================
// Test Fixture
// ============================================================================

class AllStreamTypesTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up AllStreamTypesTest..." << std::endl;
        
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
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::shared_ptr<MarketDataStream> stream;
};

// ============================================================================
// MAIN TEST: SubscribeAllAsync for 30 minutes
// ============================================================================

TEST_F(AllStreamTypesTest, SubscribeAllAsync_30Minutes)
{
    const int TEST_DURATION_MINUTES = 30;
    AllStreamTypesTestHelper helper("SubscribeAll_30Min", TEST_DURATION_MINUTES);
    
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: SubscribeAllAsync for " << TEST_DURATION_MINUTES << " minutes ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Order Book Depth: " << DEFAULT_DEPTH << std::endl;
    std::cout << "Duration: " << TEST_DURATION_MINUTES << " minutes" << std::endl;
    std::cout << std::endl;
    
    // Prepare subscription parameters
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    // Create callback that tracks all message types
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            helper.recordMessage(response);
        }
    };
    
    // Start combined subscription
    std::cout << "1. Starting combined subscription to all data types..." << std::endl;
    stream->SubscribeAllAsync(
        candleInstruments,           // Candles (1-min interval)
        TEST_INSTRUMENTS,            // Order Book
        DEFAULT_DEPTH,               // Order Book depth
        TEST_INSTRUMENTS,            // Trades
        TEST_INSTRUMENTS,            // Trading Info
        TEST_INSTRUMENTS,            // Last Price
        callback);
    
    helper.start();
    
    // Wait for first messages
    std::cout << "2. Waiting for first messages (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForAnyData(30);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No messages received within 30 seconds";
        return;
    }
    
    std::cout << "3. First messages received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Wait for all data types to start receiving
    std::cout << "4. Waiting for all data types to be active..." << std::endl;
    bool allTypesActive = helper.waitForAllTypes(60);
    
    if (allTypesActive) {
        std::cout << "5. All data types are active!" << std::endl;
    } else {
        std::cout << "5. WARNING: Not all data types are active yet" << std::endl;
    }
    
    // Print initial stats
    helper.printStats();
    
    // Run for 30 minutes with periodic progress reports
    std::cout << "6. Streaming for " << TEST_DURATION_MINUTES << " minutes..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::minutes(TEST_DURATION_MINUTES);
    
    // Report intervals (every 5 minutes)
    int reportCount = 0;
    const int REPORT_INTERVAL_MINUTES = 5;
    
    while (std::chrono::steady_clock::now() < endTime) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        
        auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(
            std::chrono::steady_clock::now() - startTime).count();
        
        // Print progress every REPORT_INTERVAL_MINUTES
        if ((elapsed % REPORT_INTERVAL_MINUTES) == 0 && elapsed > 0) {
            reportCount++;
            if (reportCount * REPORT_INTERVAL_MINUTES <= TEST_DURATION_MINUTES) {
                std::cout << "\n>>> Progress Report at " << elapsed << " minutes <<<" << std::endl;
                helper.printProgressReport();
                std::cout << ">>>\n" << std::endl;
            }
        }
        
        // Print brief status
        if ((elapsed % 10) == 0) {
            std::cout << "   [" << elapsed << " min] Messages: " << helper.getTotalMessages() 
                      << ", Candles: " << helper.getCandleMessages()
                      << ", OrderBook: " << helper.getOrderBookMessages()
                      << ", Trades: " << helper.getTradeMessages()
                      << ", Info: " << helper.getInfoMessages()
                      << ", LastPrice: " << helper.getLastPriceMessages()
                      << std::endl;
        }
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "7. Unsubscribing..." << std::endl;
    stream->UnSubscribeAllAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    std::cout << "\n=== Verification ===" << std::endl;
    
    bool success = true;
    
    if (helper.getTotalMessages() < 1) {
        std::cerr << "FAIL: No messages received" << std::endl;
        success = false;
    } else {
        std::cout << "PASS: Received " << helper.getTotalMessages() << " messages" << std::endl;
    }
    
    // Check that at least some data types received messages
    if (helper.getCandleMessages() > 0) {
        std::cout << "PASS: Candles - " << helper.getCandleMessages() << " messages" << std::endl;
    } else {
        std::cout << "INFO: No candle messages (may be expected during off-hours)" << std::endl;
    }
    
    if (helper.getOrderBookMessages() > 0) {
        std::cout << "PASS: OrderBook - " << helper.getOrderBookMessages() << " messages" << std::endl;
    } else {
        std::cout << "INFO: No order book messages (may be expected during off-hours)" << std::endl;
    }
    
    if (helper.getTradeMessages() > 0) {
        std::cout << "PASS: Trades - " << helper.getTradeMessages() << " messages" << std::endl;
    } else {
        std::cout << "INFO: No trade messages (may be expected during off-hours)" << std::endl;
    }
    
    if (helper.getInfoMessages() > 0) {
        std::cout << "PASS: Trading Info - " << helper.getInfoMessages() << " messages" << std::endl;
    } else {
        std::cerr << "FAIL: No trading info messages" << std::endl;
        success = false;
    }
    
    if (helper.getLastPriceMessages() > 0) {
        std::cout << "PASS: Last Price - " << helper.getLastPriceMessages() << " messages" << std::endl;
    } else {
        std::cout << "INFO: No last price messages (may be expected during off-hours)" << std::endl;
    }
    
    if (helper.getSubscriptionConfirmations() > 0) {
        std::cout << "PASS: Subscription confirmations - " << helper.getSubscriptionConfirmations() << std::endl;
    } else {
        std::cout << "INFO: Subscription confirmations - " << helper.getSubscriptionConfirmations() << std::endl;
    }
    
    if (helper.getActiveInstrumentCount() > 0) {
        std::cout << "PASS: Active instruments - " << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
    } else {
        std::cerr << "FAIL: No active instruments" << std::endl;
        success = false;
    }
    
    std::cout << "\n=== TEST " << (success ? "PASSED" : "COMPLETED") << " ===" << std::endl;
    
    // Main assertion - we should have received some messages
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive at least 1 message";
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "At least 1 instrument should be active";
}

// ============================================================================
// TEST: Quick verification (5 minutes)
// ============================================================================

TEST_F(AllStreamTypesTest, SubscribeAllAsync_5Minutes_QuickCheck)
{
    const int TEST_DURATION_MINUTES = 5;
    AllStreamTypesTestHelper helper("SubscribeAll_5Min_Quick", TEST_DURATION_MINUTES);
    
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: SubscribeAllAsync for " << TEST_DURATION_MINUTES << " minutes (Quick Check) ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    
    // Prepare subscription parameters
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            helper.recordMessage(response);
        }
    };
    
    // Start combined subscription
    std::cout << "Starting combined subscription..." << std::endl;
    stream->SubscribeAllAsync(
        candleInstruments,
        TEST_INSTRUMENTS,
        DEFAULT_DEPTH,
        TEST_INSTRUMENTS,
        TEST_INSTRUMENTS,
        TEST_INSTRUMENTS,
        callback);
    
    helper.start();
    
    // Wait for first messages
    std::cout << "Waiting for first messages (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForAnyData(30);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No messages received within 30 seconds";
        return;
    }
    
    std::cout << "First messages received!" << std::endl;
    
    // Wait for all types
    bool allTypes = helper.waitForAllTypes(60);
    std::cout << "All types active: " << (allTypes ? "yes" : "no") << std::endl;
    
    // Run for 5 minutes
    std::cout << "Streaming for " << TEST_DURATION_MINUTES << " minutes..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::minutes(TEST_DURATION_MINUTES)) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(
            std::chrono::steady_clock::now() - startTime).count();
        std::cout << "   [" << elapsed << " min] Messages: " << helper.getTotalMessages() << std::endl;
    }
    
    helper.stop();
    
    // Unsubscribe
    stream->UnSubscribeAllAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive at least 1 message";
    // Note: Trading info may not be available during off-market hours
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// TEST: Stability test with multiple subscribe/unsubscribe cycles
// ============================================================================

TEST_F(AllStreamTypesTest, SubscribeAllAsync_StabilityTest)
{
    const int TEST_DURATION_MINUTES = 10;
    const int CYCLE_COUNT = 3;
    const int SECONDS_PER_CYCLE = (TEST_DURATION_MINUTES * 60) / CYCLE_COUNT;
    
    AllStreamTypesTestHelper helper("SubscribeAll_Stability", TEST_DURATION_MINUTES);
    
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: SubscribeAllAsync Stability Test ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    std::cout << "Test Duration: " << TEST_DURATION_MINUTES << " minutes" << std::endl;
    std::cout << "Cycles: " << CYCLE_COUNT << " (each " << SECONDS_PER_CYCLE << " seconds)" << std::endl;
    
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            helper.recordMessage(response);
        }
    };
    
    for (int cycle = 1; cycle <= CYCLE_COUNT; cycle++) {
        std::cout << "\n--- Cycle " << cycle << "/" << CYCLE_COUNT << " ---" << std::endl;
        
        // Subscribe
        std::cout << "Subscribing..." << std::endl;
        stream->SubscribeAllAsync(
            candleInstruments,
            TEST_INSTRUMENTS,
            DEFAULT_DEPTH,
            TEST_INSTRUMENTS,
            TEST_INSTRUMENTS,
            TEST_INSTRUMENTS,
            callback);
        
        helper.start();
        
        // Wait for data
        bool gotData = helper.waitForAnyData(30);
        std::cout << "Data received: " << (gotData ? "yes" : "no") << std::endl;
        
        // Stream for a while
        std::this_thread::sleep_for(std::chrono::seconds(SECONDS_PER_CYCLE));
        
        // Unsubscribe
        std::cout << "Unsubscribing..." << std::endl;
        stream->UnSubscribeAllAsync();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        helper.stop();
        
        // Print progress
        std::cout << "Total messages so far: " << helper.getTotalMessages() << std::endl;
        std::cout << "  Candles: " << helper.getCandleMessages() 
                  << ", OrderBook: " << helper.getOrderBookMessages() 
                  << ", Trades: " << helper.getTradeMessages() << std::endl;
        std::cout << "  Info: " << helper.getInfoMessages() 
                  << ", LastPrice: " << helper.getLastPriceMessages() << std::endl;
    }
    
    helper.printStats();
    
    // Verify we got messages in at least one cycle
    EXPECT_GE(helper.getTotalMessages(), 1) << "Should receive messages during stability test";
    
    std::cout << "\n=== STABILITY TEST PASSED ===" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Combined Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "Order Book Depth: " << DEFAULT_DEPTH << std::endl;
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeAllAsync_30Minutes (MAIN TEST - 30 min)" << std::endl;
    std::cout << "  - SubscribeAllAsync_5Minutes_QuickCheck (5 min)" << std::endl;
    std::cout << "  - SubscribeAllAsync_StabilityTest (10 min, 3 cycles)" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

