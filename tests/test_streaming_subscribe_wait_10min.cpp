/**
 * @file test_streaming_subscribe_wait_10min.cpp
 * @brief MarketDataStream Subscribe and Wait Test for TinkoffInvestSDK
 * 
 * This test subscribes to market data stream and waits for 10 minutes
 * to receive incoming messages, logging detailed information for each message.
 * 
 * This is useful for:
 * - Long-running integration testing
 * - Verifying streaming connectivity over extended periods
 * - Debugging message content and payload types
 * - Load testing the streaming client
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

// Test duration: 10 minutes = 600 seconds
const int TEST_DURATION_SECONDS = 600;  // 10 minutes
// For quick testing, you can reduce this value:
// const int TEST_DURATION_SECONDS = 60;  // 1 minute for quick test

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

// Test instruments (Russian MOEX stocks)
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
// Message Statistics Helper Class
// ============================================================================

class StreamingMessageLogger {
public:
    StreamingMessageLogger(const std::string& testName)
        : testName(testName),
          totalMessages(0),
          payloadTypeMessages(),
          instrumentMessages(),
          running(false),
          startTime(std::chrono::steady_clock::now()),
          lastLogTime(std::chrono::steady_clock::now())
    {
        std::cout << "[StreamingMessageLogger] Created for test: " << testName << std::endl;
    }

    void start() {
        running = true;
        startTime = std::chrono::steady_clock::now();
        lastLogTime = startTime;
        std::cout << "[StreamingMessageLogger] Started at " << getCurrentTimestamp() << std::endl;
    }

    void stop() {
        running = false;
        std::cout << "[StreamingMessageLogger] Stopped at " << getCurrentTimestamp() << std::endl;
    }

    void logMessage(const MarketDataStreamResponse& response) {
        std::lock_guard<std::mutex> lock(mutex);
        
        totalMessages++;
        
        // Get payload type
        auto payloadType = response.getPayloadType();
        std::string payloadTypeStr = response.getPayloadTypeString();
        payloadTypeMessages[payloadTypeStr]++;
        
        std::string instrumentId;
        std::string messageDetails;
        
        // Extract instrument ID and details based on payload type
        switch (payloadType) {
            case MarketDataStreamResponse::PayloadType::LAST_PRICE: {
                auto lastPrice = response.getLastPrice();
                instrumentId = lastPrice.figi();
                double price = lastPrice.price().units() + lastPrice.price().nano() / 1e9;
                messageDetails = "price=" + std::to_string(price);
                lastPrices[instrumentId] = price;
                break;
            }
            case MarketDataStreamResponse::PayloadType::CANDLE: {
                auto candle = response.getCandle();
                instrumentId = candle.figi();
                double open = candle.open().units() + candle.open().nano() / 1e9;
                double close = candle.close().units() + candle.close().nano() / 1e9;
                messageDetails = "open=" + std::to_string(open) + ", close=" + std::to_string(close);
                break;
            }
            case MarketDataStreamResponse::PayloadType::TRADE: {
                auto trade = response.getTrade();
                instrumentId = trade.figi();
                double price = trade.price().units() + trade.price().nano() / 1e9;
                messageDetails = "price=" + std::to_string(price) + ", quantity=" + std::to_string(trade.quantity());
                trades[instrumentId]++;
                break;
            }
            case MarketDataStreamResponse::PayloadType::ORDERBOOK: {
                auto orderbook = response.getOrderBook();
                instrumentId = orderbook.figi();
                messageDetails = "bids=" + std::to_string(orderbook.bids_size()) + 
                               ", asks=" + std::to_string(orderbook.asks_size());
                orderbooks[instrumentId]++;
                break;
            }
            case MarketDataStreamResponse::PayloadType::TRADING_STATUS: {
                auto tradingStatus = response.getTradingStatus();
                instrumentId = tradingStatus.figi();
                messageDetails = "trading_status=" + std::to_string(tradingStatus.trading_status());
                tradingStatuses[instrumentId] = tradingStatus.trading_status();
                break;
            }
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_CANDLES_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_TRADES_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE:
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE: {
                instrumentId = "(subscription response)";
                messageDetails = "tracking_id=" + response.getTrackingId();
                break;
            }
            case MarketDataStreamResponse::PayloadType::PING: {
                instrumentId = "(ping)";
                messageDetails = "ping";
                break;
            }
            default:
                instrumentId = "(unknown)";
                messageDetails = "unknown payload type";
                break;
        }
        
        instrumentMessages[instrumentId]++;
        
        // Log message details to console
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();
        
        std::cout << "[" << std::setfill('0') << std::setw(4) << elapsed << "s] "
                  << "MSG #" << totalMessages << " | "
                  << "Type: " << std::setfill(' ') << std::setw(30) << std::left << payloadTypeStr << " | "
                  << "FIGI: " << instrumentId << " | "
                  << messageDetails
                  << std::endl;
        
        // Log periodic summary every 30 seconds
        auto timeSinceLastLog = std::chrono::duration_cast<std::chrono::seconds>(now - lastLogTime).count();
        if (timeSinceLastLog >= 30) {
            printPeriodicSummary();
            lastLogTime = now;
        }
        
        cv.notify_all();
    }

    void logSubscriptionConfirmation(const std::string& trackingId) {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "\n========================================" << std::endl;
        std::cout << "SUBSCRIPTION CONFIRMED" << std::endl;
        std::cout << "Tracking ID: " << trackingId << std::endl;
        std::cout << "========================================\n" << std::endl;
    }

    bool waitForMessages(int minMessages, int maxSeconds = 60) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this, minMessages]() { 
                              return totalMessages >= minMessages; 
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

    auto getElapsedSeconds() const {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }

    const std::string& getName() const { return testName; }
    bool isRunning() const { return running.load(); }

    void printPeriodicSummary() {
        std::cout << "\n--- Periodic Summary (" << getElapsedSeconds() << "s) ---" << std::endl;
        std::cout << "Total messages: " << totalMessages << std::endl;
        std::cout << "Active instruments: " << instrumentMessages.size() << std::endl;
        
        if (!payloadTypeMessages.empty()) {
            std::cout << "Payload types received:" << std::endl;
            for (const auto& [type, count] : payloadTypeMessages) {
                std::cout << "  - " << type << ": " << count << std::endl;
            }
        }
        std::cout << "-------------------------------\n" << std::endl;
    }

    void printFinalSummary() const {
        std::lock_guard<std::mutex> lock(mutex);
        
        std::cout << "\n\n";
        std::cout << "########################################" << std::endl;
        std::cout << "#       FINAL TEST SUMMARY            #" << std::endl;
        std::cout << "########################################" << std::endl;
        std::cout << "Test Name: " << testName << std::endl;
        std::cout << "Duration: " << getElapsedSeconds() << " seconds" << std::endl;
        std::cout << "Total Messages Received: " << totalMessages << std::endl;
        
        if (totalMessages > 0) {
            double msgPerSec = static_cast<double>(totalMessages) / getElapsedSeconds();
            std::cout << "Average Message Rate: " << std::fixed << std::setprecision(2) 
                      << msgPerSec << " messages/second" << std::endl;
        }
        
        std::cout << "\n--- Messages by Payload Type ---" << std::endl;
        for (const auto& [type, count] : payloadTypeMessages) {
            std::cout << "  " << type << ": " << count << std::endl;
        }
        
        std::cout << "\n--- Messages by Instrument (FIGI) ---" << std::endl;
        for (const auto& [figi, count] : instrumentMessages) {
            std::cout << "  " << figi << ": " << count << " messages" << std::endl;
        }
        
        if (!lastPrices.empty()) {
            std::cout << "\n--- Latest Prices ---" << std::endl;
            for (const auto& [figi, price] : lastPrices) {
                std::cout << "  " << figi << ": " << std::fixed << std::setprecision(2) 
                          << price << std::endl;
            }
        }
        
        if (!trades.empty()) {
            std::cout << "\n--- Trade Counts ---" << std::endl;
            for (const auto& [figi, count] : trades) {
                std::cout << "  " << figi << ": " << count << " trades" << std::endl;
            }
        }
        
        if (!orderbooks.empty()) {
            std::cout << "\n--- Order Book Updates ---" << std::endl;
            for (const auto& [figi, count] : orderbooks) {
                std::cout << "  " << figi << ": " << count << " updates" << std::endl;
            }
        }
        
        std::cout << "\n########################################" << std::endl;
    }

private:
    std::string testName;
    std::atomic<int> totalMessages;
    std::atomic<bool> running;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::chrono::steady_clock::time_point startTime;
    std::chrono::steady_clock::time_point lastLogTime;
    
    // Statistics
    std::unordered_map<std::string, int> payloadTypeMessages;
    std::unordered_map<std::string, int> instrumentMessages;
    std::unordered_map<std::string, double> lastPrices;
    std::unordered_map<std::string, int> trades;
    std::unordered_map<std::string, int> orderbooks;
    std::unordered_map<std::string, int> tradingStatuses;

    std::string getCurrentTimestamp() const {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::tm tm = *std::localtime(&time);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
};

// ============================================================================
// Test Fixture
// ============================================================================

class MarketDataStreamSubscribeWaitTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "======================================================" << std::endl;
        std::cout << "Setting up MarketDataStreamSubscribeWaitTest..." << std::endl;
        std::cout << "======================================================" << std::endl;
        
        // Get token from file or environment variable
        std::string token = getApiToken();
        if (token.empty()) {
            std::cerr << "WARNING: No API token found. Set TINKOFF_TOKEN environment variable or create .test_token.txt file." << std::endl;
            std::cerr << "Test will likely fail without valid token." << std::endl;
        } else {
            std::cout << "API token found (length: " << token.length() << " characters)" << std::endl;
        }
        
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, token);
        std::cout << "MarketDataStream created successfully" << std::endl;
        
        std::cout << "Test instruments:" << std::endl;
        for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
            std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                      << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
        }
    }

    void TearDown() override {
        std::cout << "\nTearing down test..." << std::endl;
        if (stream) {
            try {
                std::cout << "Closing stream..." << std::endl;
                stream->close();
                std::cout << "Stream closed successfully" << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "Error closing stream: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "Unknown error closing stream" << std::endl;
            }
        }
        // Give time for cleanup between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    std::shared_ptr<MarketDataStream> stream;
};

// ============================================================================
// Main Test: Subscribe to Last Price and Wait 10 Minutes
// ============================================================================

TEST_F(MarketDataStreamSubscribeWaitTest, SubscribeLastPrice_Wait10Minutes_LogAllMessages)
{
    StreamingMessageLogger logger("SubscribeLastPrice_Wait10Minutes");
    
    std::cout << "\n####################################################" << std::endl;
    std::cout << "#  TEST: Subscribe Last Price and Wait 10 Minutes  #" << std::endl;
    std::cout << "####################################################" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "  Host: " << TEST_HOST << std::endl;
    std::cout << "  Instruments: " << TEST_INSTRUMENTS.size() << " instruments" << std::endl;
    std::cout << "  Duration: " << TEST_DURATION_SECONDS << " seconds (10 minutes)" << std::endl;
    std::cout << "  Start Time: " << logger.getName() << std::endl;
    std::cout << "\nThis test will:" << std::endl;
    std::cout << "  1. Subscribe to last price for " << TEST_INSTRUMENTS.size() << " instruments" << std::endl;
    std::cout << "  2. Wait for " << TEST_DURATION_SECONDS << " seconds receiving messages" << std::endl;
    std::cout << "  3. Log every message with detailed information" << std::endl;
    std::cout << "  4. Print summary statistics at the end" << std::endl;
    std::cout << "\nPress Ctrl+C to stop early if needed." << std::endl;
    std::cout << "\n";
    
    // Create callback that logs all messages
    auto callback = [&logger](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            
            // Log subscription confirmations separately
            if (response.isSubscriptionConfirmation()) {
                logger.logSubscriptionConfirmation(response.getTrackingId());
            }
            
            // Log all messages
            logger.logMessage(response);
        } else {
            std::cerr << "[WARNING] Received reply without MarketDataStreamResponse" << std::endl;
        }
    };
    
    // Start subscription
    std::cout << ">>> Starting subscription to last price..." << std::endl;
    stream->SubscribeLastPriceAsync(TEST_INSTRUMENTS, callback);
    
    logger.start();
    
    // Wait for first message (connection confirmation)
    std::cout << ">>> Waiting for first message (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = logger.waitForMessages(1, 30);
    
    if (!gotFirstMessage) {
        logger.printFinalSummary();
        std::cout << "\n[RESULT] No messages received within 30 seconds." << std::endl;
        std::cout << "This may be due to:" << std::endl;
        std::cout << "  - Market is closed" << std::endl;
        std::cout << "  - Invalid API token" << std::endl;
        std::cout << "  - Network issues" << std::endl;
        std::cout << "  - Subscription failed" << std::endl;
        GTEST_FAIL() << "No messages received within 30 seconds - possible subscription failure";
        return;
    }
    
    std::cout << ">>> First message received! Starting 10-minute timer..." << std::endl;
    std::cout << ">>> Streaming messages... (logging every message)" << std::endl;
    std::cout << "\n";
    
    // Wait for the configured duration
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(TEST_DURATION_SECONDS);
    
    while (std::chrono::steady_clock::now() < endTime) {
        auto remaining = std::chrono::duration_cast<std::chrono::seconds>(endTime - std::chrono::steady_clock::now()).count();
        
        // Print progress every 60 seconds
        if (remaining % 60 == 0 && remaining != TEST_DURATION_SECONDS) {
            int elapsed = TEST_DURATION_SECONDS - remaining;
            std::cout << "\n>>> [" << elapsed << "s / " << TEST_DURATION_SECONDS << "s] "
                      << "Messages so far: " << logger.getTotalMessages() 
                      << " | Active instruments: " << logger.getActiveInstrumentCount()
                      << " | " << remaining << "s remaining..."
                      << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    logger.stop();
    
    // Unsubscribe
    std::cout << "\n>>> Unsubscribing..." << std::endl;
    stream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    // Print final summary
    logger.printFinalSummary();
    
    // Verify results
    std::cout << "\n[RESULT] Test completed!" << std::endl;
    if (logger.getTotalMessages() > 0) {
        std::cout << "[PASS] Received " << logger.getTotalMessages() << " messages" << std::endl;
        SUCCEED();
    } else {
        std::cout << "[FAIL] No messages received during test" << std::endl;
        GTEST_FAIL() << "No messages received during 10-minute test";
    }
}

// ============================================================================
// Test: Subscribe to Trades and Wait 10 Minutes
// ============================================================================

TEST_F(MarketDataStreamSubscribeWaitTest, SubscribeTrades_Wait10Minutes_LogAllMessages)
{
    StreamingMessageLogger logger("SubscribeTrades_Wait10Minutes");
    
    std::cout << "\n################################################" << std::endl;
    std::cout << "#   TEST: Subscribe Trades and Wait 10 Minutes  #" << std::endl;
    std::cout << "################################################" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "  Host: " << TEST_HOST << std::endl;
    std::cout << "  Instruments: " << TEST_INSTRUMENTS.size() << " instruments" << std::endl;
    std::cout << "  Duration: " << TEST_DURATION_SECONDS << " seconds" << std::endl;
    
    // Create callback that logs all messages
    auto callback = [&logger](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            
            if (response.isSubscriptionConfirmation()) {
                logger.logSubscriptionConfirmation(response.getTrackingId());
            }
            
            logger.logMessage(response);
        }
    };
    
    // Start subscription
    std::cout << "\n>>> Starting subscription to trades..." << std::endl;
    stream->SubscribeTradesAsync(TEST_INSTRUMENTS, callback);
    
    logger.start();
    
    // Wait for first message
    std::cout << ">>> Waiting for first message (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = logger.waitForMessages(1, 30);
    
    if (!gotFirstMessage) {
        logger.printFinalSummary();
        GTEST_FAIL() << "No trade messages received within 30 seconds";
        return;
    }
    
    std::cout << ">>> First message received! Starting timer..." << std::endl;
    
    // Wait for configured duration
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(TEST_DURATION_SECONDS);
    
    while (std::chrono::steady_clock::now() < endTime) {
        auto remaining = std::chrono::duration_cast<std::chrono::seconds>(endTime - std::chrono::steady_clock::now()).count();
        
        if (remaining % 60 == 0 && remaining != TEST_DURATION_SECONDS) {
            int elapsed = TEST_DURATION_SECONDS - remaining;
            std::cout << "\n>>> [" << elapsed << "s / " << TEST_DURATION_SECONDS << "s] "
                      << "Trades: " << logger.getTotalMessages() << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    logger.stop();
    
    // Unsubscribe
    std::cout << "\n>>> Unsubscribing..." << std::endl;
    stream->UnSubscribeTradesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    logger.printFinalSummary();
    
    std::cout << "\n[RESULT] Trades test completed with " << logger.getTotalMessages() << " messages" << std::endl;
}

// ============================================================================
// Test: Subscribe to Candles and Wait 10 Minutes
// ============================================================================

TEST_F(MarketDataStreamSubscribeWaitTest, SubscribeCandles_Wait10Minutes_LogAllMessages)
{
    StreamingMessageLogger logger("SubscribeCandles_Wait10Minutes");
    
    std::cout << "\n################################################" << std::endl;
    std::cout << "#   TEST: Subscribe Candles and Wait 10 Minutes  #" << std::endl;
    std::cout << "################################################" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "  Host: " << TEST_HOST << std::endl;
    std::cout << "  Instruments: " << TEST_INSTRUMENTS.size() << " instruments" << std::endl;
    std::cout << "  Interval: 5 minutes" << std::endl;
    std::cout << "  Duration: " << TEST_DURATION_SECONDS << " seconds" << std::endl;
    
    // Prepare candle subscriptions
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SUBSCRIPTION_INTERVAL_FIVE_MINUTES});
    }
    
    // Create callback that logs all messages
    auto callback = [&logger](ServiceReply reply) {
        if (reply.hasMarketDataStreamResponse()) {
            auto response = reply.getMarketDataStreamResponse();
            
            if (response.isSubscriptionConfirmation()) {
                logger.logSubscriptionConfirmation(response.getTrackingId());
            }
            
            logger.logMessage(response);
        }
    };
    
    // Start subscription
    std::cout << "\n>>> Starting subscription to candles..." << std::endl;
    stream->SubscribeCandlesAsync(candleInstruments, callback);
    
    logger.start();
    
    // Wait for first message
    std::cout << ">>> Waiting for first message (max 30 seconds)..." << std::endl;
    bool gotFirstMessage = logger.waitForMessages(1, 30);
    
    if (!gotFirstMessage) {
        logger.printFinalSummary();
        GTEST_FAIL() << "No candle messages received within 30 seconds";
        return;
    }
    
    std::cout << ">>> First message received! Starting timer..." << std::endl;
    
    // Wait for configured duration
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(TEST_DURATION_SECONDS);
    
    while (std::chrono::steady_clock::now() < endTime) {
        auto remaining = std::chrono::duration_cast<std::chrono::seconds>(endTime - std::chrono::steady_clock::now()).count();
        
        if (remaining % 60 == 0 && remaining != TEST_DURATION_SECONDS) {
            int elapsed = TEST_DURATION_SECONDS - remaining;
            std::cout << "\n>>> [" << elapsed << "s / " << TEST_DURATION_SECONDS << "s] "
                      << "Candles: " << logger.getTotalMessages() << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    logger.stop();
    
    // Unsubscribe
    std::cout << "\n>>> Unsubscribing..." << std::endl;
    stream->UnSubscribeCandlesAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    logger.printFinalSummary();
    
    std::cout << "\n[RESULT] Candles test completed with " << logger.getTotalMessages() << " messages" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK MarketDataStream Subscribe & Wait Test" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "  Host: " << TEST_HOST << std::endl;
    std::cout << "  Duration: " << TEST_DURATION_SECONDS << " seconds (10 minutes)" << std::endl;
    std::cout << "  Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "    " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - SubscribeLastPrice_Wait10Minutes_LogAllMessages" << std::endl;
    std::cout << "  - SubscribeTrades_Wait10Minutes_LogAllMessages" << std::endl;
    std::cout << "  - SubscribeCandles_Wait10Minutes_LogAllMessages" << std::endl;
    std::cout << std::endl;
    std::cout << "NOTE: To change test duration, edit TEST_DURATION_SECONDS constant" << std::endl;
    std::cout << "      Default is 600 seconds (10 minutes)" << std::endl;
    std::cout << "      For quick test, set to 60 seconds" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

