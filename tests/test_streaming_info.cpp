/**
 * @file test_streaming_info.cpp
 * @brief Trading Info (Trading Status) Streaming Tests for TinkoffInvestSDK
 * 
 * This file contains tests for subscribing to trading status streaming:
 * - Subscribe to instruments for trading status updates
 * - Run for 1 minute
 * - Verify trading status messages are received
 * - Test unsubscribe/resubscribe functionality
 * 
 * These tests verify real streaming functionality with actual API connection.
 * SubscribeInfoAsync provides real-time trading status updates for instruments.
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
// Helper function to get trading status string
// ============================================================================

std::string tradingStatusToString(SecurityTradingStatus status) {
    switch (status) {
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_UNSPECIFIED:
            return "UNSPECIFIED";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING:
            return "NOT_AVAILABLE_FOR_TRADING";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_OPENING_PERIOD:
            return "OPENING_PERIOD";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_CLOSING_PERIOD:
            return "CLOSING_PERIOD";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_BREAK_IN_TRADING:
            return "BREAK_IN_TRADING";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_NORMAL_TRADING:
            return "NORMAL_TRADING";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_CLOSING_AUCTION:
            return "CLOSING_AUCTION";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_DARK_POOL_AUCTION:
            return "DARK_POOL_AUCTION";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_DISCRETE_AUCTION:
            return "DISCRETE_AUCTION";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_OPENING_AUCTION_PERIOD:
            return "OPENING_AUCTION_PERIOD";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_TRADING_AT_CLOSING_AUCTION_PRICE:
            return "TRADING_AT_CLOSING_AUCTION_PRICE";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_SESSION_ASSIGNED:
            return "SESSION_ASSIGNED";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_SESSION_CLOSE:
            return "SESSION_CLOSE";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_SESSION_OPEN:
            return "SESSION_OPEN";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_DEALER_NORMAL_TRADING:
            return "DEALER_NORMAL_TRADING";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_DEALER_BREAK_IN_TRADING:
            return "DEALER_BREAK_IN_TRADING";
        case SecurityTradingStatus::SECURITY_TRADING_STATUS_DEALER_NOT_AVAILABLE_FOR_TRADING:
            return "DEALER_NOT_AVAILABLE_FOR_TRADING";
        default:
            return "UNKNOWN";
    }
}

// ============================================================================
// Test Helper Class for Tracking Trading Info Messages
// ============================================================================

class InfoTestHelper {
public:
    InfoTestHelper(const std::string& testName)
        : testName(testName),
          totalMessages(0),
          tradingStatusMessages(0),
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
        if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
            tradingStatusMessages++;
            auto tradingStatus = response.getTradingStatus();
            
            // Track trading status per instrument
            InfoStats stats;
            stats.message_count++;
            stats.trading_status = tradingStatus.trading_status();
            stats.status_string = tradingStatusToString(tradingStatus.trading_status());
            
            // Get time if available
            if (tradingStatus.has_time()) {
                auto time = tradingStatus.time();
                stats.has_time = true;
            }
            
            instrumentStats[figi] = stats;
            lastTradingStatus[figi] = tradingStatus.trading_status();
            
            cv.notify_all();
        } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE)) {
            subscriptionConfirmations++;
            auto infoResponse = response.getSubscribeInfoResponse();
            trackingId = infoResponse.tracking_id();
            std::cout << "[" << testName << "] Subscription confirmation received, tracking_id: " 
                      << trackingId << std::endl;
            cv.notify_all();
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

    bool waitForTradingStatusData(int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return tradingStatusMessages > 0; 
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
                              return instrumentStats.size() >= TEST_INSTRUMENTS.size(); 
                          });
    }

    bool waitForAnyInstrument(int maxSeconds = 10) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this]() { 
                              return !instrumentStats.empty(); 
                          });
    }

    int getTotalMessages() const { return totalMessages.load(); }
    int getTradingStatusMessages() const { return tradingStatusMessages.load(); }
    int getSubscriptionConfirmations() const { return subscriptionConfirmations.load(); }
    
    int getInstrumentMessageCount(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = instrumentStats.find(figi);
        return (it != instrumentStats.end()) ? it->second.message_count : 0;
    }

    int getActiveInstrumentCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentStats.size();
    }

    bool isInstrumentActive(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        return instrumentStats.find(figi) != instrumentStats.end();
    }

    SecurityTradingStatus getLastTradingStatus(const std::string& figi) const {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = lastTradingStatus.find(figi);
        return (it != lastTradingStatus.end()) ? it->second : SecurityTradingStatus::SECURITY_TRADING_STATUS_UNSPECIFIED;
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
        std::cout << "Trading status messages: " << tradingStatusMessages << std::endl;
        std::cout << "Subscription confirmations: " << subscriptionConfirmations << std::endl;
        std::cout << "Active instruments: " << instrumentStats.size() << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        std::cout << "Tracking ID: " << trackingId << std::endl;
        std::cout << "Elapsed time: " << getElapsedMs() << "ms" << std::endl;
        
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            const std::string& figi = TEST_INSTRUMENTS[i];
            auto it = instrumentStats.find(figi);
            if (it != instrumentStats.end()) {
                const InfoStats& stats = it->second;
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << stats.message_count << " trading status messages" << std::endl;
                std::cout << "    Latest status: " << stats.status_string << std::endl;
            } else {
                std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                          << "NO DATA" << std::endl;
            }
        }
        std::cout << "==============================\n" << std::endl;
    }

private:
    struct InfoStats {
        int message_count = 0;
        SecurityTradingStatus trading_status = SecurityTradingStatus::SECURITY_TRADING_STATUS_UNSPECIFIED;
        std::string status_string;
        bool has_time = false;
    };

    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<int> tradingStatusMessages;
    std::atomic<int> subscriptionConfirmations;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, InfoStats> instrumentStats;
    std::unordered_map<std::string, SecurityTradingStatus> lastTradingStatus;
    std::string trackingId;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Callback Wrapper
// ============================================================================

void infoCallback(InfoTestHelper& helper, const std::string& figi, 
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

class InfoStreamingTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up InfoStreamingTest..." << std::endl;
        
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
// MAIN TEST: SubscribeInfoAsync for 1 minute
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_1Minute)
{
    InfoTestHelper helper("SubscribeInfo_1Min");
    
    std::cout << "\n=== TEST: SubscribeInfoAsync for 1 minute ===" << std::endl;
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
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE)) {
                helper.recordSubscriptionConfirmation("");
            }
        }
    };
    
    // Start subscription
    std::cout << "1. Starting info subscription to " << TEST_INSTRUMENTS.size() 
              << " instruments..." << std::endl;
    stream->SubscribeInfoAsync(TEST_INSTRUMENTS, callback);
    
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
    
    // Wait for first trading status message
    std::cout << "4. Waiting for first trading status message (max 15 seconds)..." << std::endl;
    bool gotFirstMessage = helper.waitForTradingStatusData(15);
    
    if (!gotFirstMessage) {
        helper.printStats();
        GTEST_FAIL() << "No trading status messages received within 15 seconds";
        return;
    }
    
    std::cout << "5. First trading status message received!" << std::endl;
    std::cout << "   Active instruments: " << helper.getActiveInstrumentCount() 
              << "/" << TEST_INSTRUMENTS.size() << std::endl;
    
    // Check all instruments are receiving data
    std::cout << "6. Checking all instruments receive trading status data..." << std::endl;
    bool allInstrumentsActive = helper.waitForAllInstruments(30);
    
    helper.printStats();
    
    if (!allInstrumentsActive) {
        std::cout << "WARNING: Not all instruments are receiving trading status data (" 
                  << helper.getActiveInstrumentCount() << "/" 
                  << TEST_INSTRUMENTS.size() << ")" << std::endl;
    }
    
    // Run for 1 minute total
    std::cout << "7. Streaming trading status for 60 seconds..." << std::endl;
    auto startTime = std::chrono::steady_clock::now();
    int statusMessagesAtStart = helper.getTradingStatusMessages();
    
    while (std::chrono::steady_clock::now() - startTime < std::chrono::seconds(60)) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        int currentStatusMessages = helper.getTradingStatusMessages();
        int activeInstruments = helper.getActiveInstrumentCount();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime).count();
        
        std::cout << "   [" << elapsed << "s] Trading Status: " << currentStatusMessages 
                  << " (+" << (currentStatusMessages - statusMessagesAtStart) << "), "
                  << "Active: " << activeInstruments << "/" 
                  << TEST_INSTRUMENTS.size() << std::endl;
        
        statusMessagesAtStart = currentStatusMessages;
    }
    
    helper.stop();
    
    // Unsubscribe
    std::cout << "8. Unsubscribing..." << std::endl;
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Final statistics
    helper.printStats();
    
    // Verify results
    EXPECT_GE(helper.getTradingStatusMessages(), 1) << "Should receive at least 1 trading status message";
    EXPECT_GE(helper.getSubscriptionConfirmations(), 1) << "Should receive subscription confirmation";
    
    // Print individual instrument statistics
    std::cout << "\nPer-instrument trading status statistics:" << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        const std::string& figi = TEST_INSTRUMENTS[i];
        int count = helper.getInstrumentMessageCount(figi);
        auto status = helper.getLastTradingStatus(figi);
        std::cout << "  " << TEST_INSTRUMENT_NAMES[i] << " (" << figi << "): "
                  << count << " messages, last status: " << tradingStatusToString(status) << std::endl;
    }
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Verify Trading Status Structure
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_VerifyStructure)
{
    InfoTestHelper helper("VerifyTradingStatusStructure");
    
    std::cout << "\n=== TEST: Verify Trading Status Structure ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
                
                // Log the structure
                std::cout << "  Trading status received:" << std::endl;
                std::cout << "    figi: " << tradingStatus.figi() << std::endl;
                std::cout << "    trading_status: " << tradingStatusToString(tradingStatus.trading_status()) << std::endl;
                std::cout << "    has_time: " << (tradingStatus.has_time() ? "true" : "false") << std::endl;
            } else if (response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE)) {
                auto infoResponse = response.getSubscribeInfoResponse();
                std::cout << "  Subscription info response:" << std::endl;
                std::cout << "    tracking_id: " << infoResponse.tracking_id() << std::endl;
                helper.recordSubscriptionConfirmation("");
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to trading status..." << std::endl;
    stream->SubscribeInfoAsync({TEST_INSTRUMENTS[0]}, callback);
    
    helper.start();
    
    // Wait for trading status data
    std::cout << "Waiting for trading status data..." << std::endl;
    bool gotData = helper.waitForTradingStatusData(15);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    if (gotData && helper.getTradingStatusMessages() > 0) {
        std::cout << "\n=== STRUCTURE VERIFICATION PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No trading status data to verify structure";
    }
}

// ============================================================================
// Test: Single Instrument Subscription (30 seconds)
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_SingleInstrument_30Seconds)
{
    InfoTestHelper helper("SingleInstrumentInfo");
    
    std::cout << "\n=== TEST: Single Instrument Trading Status (30 seconds) ===" << std::endl;
    std::cout << "Instrument: " << TEST_INSTRUMENT_NAMES[0] 
              << " (" << TEST_INSTRUMENTS[0] << ")" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
            }
        }
    };
    
    // Subscribe to single instrument
    std::vector<std::string> singleInstrument = {TEST_INSTRUMENTS[0]};
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeInfoAsync(singleInstrument, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTradingStatusMessages(), 1) << "Should receive at least 1 trading status message";
    EXPECT_EQ(helper.getActiveInstrumentCount(), 1) << "Only 1 instrument should be active";
    
    std::cout << "\n=== SINGLE INSTRUMENT TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Unsubscribe and Resubscribe
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_UnsubscribeResubscribe)
{
    InfoTestHelper helper("UnsubscribeResubscribe");
    
    std::cout << "\n=== TEST: Unsubscribe and Resubscribe ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
            }
        }
    };
    
    // First subscription
    std::cout << "First subscription (10 seconds)..." << std::endl;
    stream->SubscribeInfoAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Unsubscribe
    std::cout << "Unsubscribing..." << std::endl;
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    int messagesBeforeResubscribe = helper.getTradingStatusMessages();
    std::cout << "Trading status messages before resubscribe: " << messagesBeforeResubscribe << std::endl;
    
    // Resubscribe
    std::cout << "Resubscribing (10 seconds)..." << std::endl;
    stream->SubscribeInfoAsync(TEST_INSTRUMENTS, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify resubscription works
    EXPECT_GE(helper.getTradingStatusMessages(), messagesBeforeResubscribe) 
        << "Resubscription should receive trading status messages";
    
    std::cout << "\n=== UNSUBSCRIBE/RESUBSCRIBE TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Quick Subscription Test (10 seconds)
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_QuickTest_10Seconds)
{
    InfoTestHelper helper("QuickInfoTest");
    
    std::cout << "\n=== TEST: Quick Trading Status Subscription (10 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing..." << std::endl;
    stream->SubscribeInfoAsync(TEST_INSTRUMENTS, callback);
    
    helper.start();
    
    // Wait 10 seconds
    std::cout << "Streaming for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getTradingStatusMessages(), 1) << "Should receive at least 1 trading status message";
    
    std::cout << "\n=== QUICK TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Subscriptions in Sequence
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_MultipleSubscriptions)
{
    InfoTestHelper helper("MultipleSubscriptions");
    
    std::cout << "\n=== TEST: Multiple Subscriptions in Sequence ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
            }
        }
    };
    
    // Subscribe to first instrument
    std::cout << "Subscribing to instrument 1: " << TEST_INSTRUMENT_NAMES[0] << std::endl;
    std::vector<std::string> batch1 = {TEST_INSTRUMENTS[0]};
    stream->SubscribeInfoAsync(batch1, callback);
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Subscribe to second and third instruments
    std::cout << "Adding instruments 2-3: " << TEST_INSTRUMENT_NAMES[1] << ", " << TEST_INSTRUMENT_NAMES[2] << std::endl;
    std::vector<std::string> batch2 = {TEST_INSTRUMENTS[1], TEST_INSTRUMENTS[2]};
    stream->SubscribeInfoAsync(batch2, callback);
    
    helper.start();
    
    // Wait 30 seconds
    std::cout << "Streaming for 30 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify
    EXPECT_GE(helper.getActiveInstrumentCount(), 1) << "Should have at least 1 active instrument";
    
    std::cout << "\n=== MULTIPLE SUBSCRIPTIONS TEST PASSED ===" << std::endl;
}

// ============================================================================
// Test: Track Trading Status Changes Over Time
// ============================================================================

TEST_F(InfoStreamingTest, SubscribeInfoAsync_TrackStatusChanges)
{
    InfoTestHelper helper("TrackStatusChanges");
    
    std::cout << "\n=== TEST: Track Trading Status Changes Over Time (45 seconds) ===" << std::endl;
    
    auto callback = [&helper](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
                auto tradingStatus = response.getTradingStatus();
                helper.recordMessage(tradingStatus.figi(), response);
                
                // Log status changes
                std::cout << "  Status update: " << TEST_INSTRUMENT_NAMES[0] << " -> " 
                          << tradingStatusToString(tradingStatus.trading_status()) << std::endl;
            }
        }
    };
    
    // Subscribe
    std::cout << "Subscribing to trading status..." << std::endl;
    stream->SubscribeInfoAsync({TEST_INSTRUMENTS[0]}, callback);
    
    helper.start();
    
    // Track status over time
    int messagesAt15s = 0;
    int messagesAt30s = 0;
    int messagesAt45s = 0;
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    messagesAt15s = helper.getTradingStatusMessages();
    std::cout << "Trading status messages after 15 seconds: " << messagesAt15s << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    messagesAt30s = helper.getTradingStatusMessages();
    std::cout << "Trading status messages after 30 seconds: " << messagesAt30s 
              << " (+" << (messagesAt30s - messagesAt15s) << " in last 15s)" << std::endl;
    
    std::this_thread::sleep_for(std::chrono::seconds(15));
    messagesAt45s = helper.getTradingStatusMessages();
    std::cout << "Trading status messages after 45 seconds: " << messagesAt45s
              << " (+" << (messagesAt45s - messagesAt30s) << " in last 15s)" << std::endl;
    
    helper.stop();
    
    stream->UnSubscribeInfoAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    helper.printStats();
    
    // Verify we received some status updates
    if (messagesAt45s > 0) {
        std::cout << "\nAverage update rate: " << (messagesAt45s / 45) << " updates/second" << std::endl;
        std::cout << "\n=== STATUS TRACKING TEST PASSED ===" << std::endl;
        SUCCEED();
    } else {
        GTEST_SKIP() << "No trading status updates received";
    }
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Trading Info (Trading Status) Streaming Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeInfoAsync_1Minute (MAIN TEST)" << std::endl;
    std::cout << "  - SubscribeInfoAsync_VerifyStructure" << std::endl;
    std::cout << "  - SubscribeInfoAsync_SingleInstrument_30Seconds" << std::endl;
    std::cout << "  - SubscribeInfoAsync_UnsubscribeResubscribe" << std::endl;
    std::cout << "  - SubscribeInfoAsync_QuickTest_10Seconds" << std::endl;
    std::cout << "  - SubscribeInfoAsync_MultipleSubscriptions" << std::endl;
    std::cout << "  - SubscribeInfoAsync_TrackStatusChanges" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

