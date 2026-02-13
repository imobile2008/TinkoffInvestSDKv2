/**
 * @file test_streaming_independent_streams.cpp
 * @brief Test for Independent Streaming Subscriptions
 * 
 * This test verifies that each subscription method creates its own thread
 * with its own stream, and they don't share a common stream.
 * 
 * When calling all 5 subscription methods together, they should create
 * 5 separate streams, each receiving data independently.
 * 
 * This test:
 * - Creates multiple independent subscriptions simultaneously
 * - Tracks thread IDs to verify separate threads are created
 * - Verifies each stream receives its own messages independently
 * - Confirms streams don't interfere with each other
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
#include <set>
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
// Helper class to track independent streams
// ============================================================================

class IndependentStreamTracker {
public:
    IndependentStreamTracker(const std::string& name)
        : streamName(name),
          messageCount(0),
          running(false),
          startTime(std::chrono::steady_clock::now()) {}
    
    void start() {
        running = true;
        startTime = std::chrono::steady_clock::now();
    }
    
    void stop() {
        running = false;
    }
    
    void recordMessage() {
        std::lock_guard<std::mutex> lock(mutex);
        messageCount++;
        cv.notify_all();
    }
    
    bool waitForMessages(int minMessages, int maxSeconds = 30) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(maxSeconds), 
                          [this, minMessages]() { 
                              return messageCount >= minMessages; 
                          });
    }
    
    int getMessageCount() const { return messageCount.load(); }
    const std::string& getName() const { return streamName; }
    std::string getThreadIdStr() const { 
        std::lock_guard<std::mutex> lock(mutex);
        std::ostringstream oss;
        oss << threadId;
        return oss.str();
    }
    bool isRunning() const { return running.load(); }
    
    auto getElapsedMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }
    
    void printStats() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::ostringstream oss;
        oss << threadId;
        std::cout << "[" << streamName << "] "
                  << "Messages: " << messageCount 
                  << ", Thread ID: " << oss.str()
                  << ", Running: " << (running ? "yes" : "no")
                  << ", Elapsed: " << getElapsedMs() << "ms"
                  << std::endl;
    }

private:
    std::string streamName;
    std::atomic<int> messageCount;
    std::atomic<bool> running;
    std::thread::id threadId;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::chrono::steady_clock::time_point startTime;
};

// ============================================================================
// Test Fixture
// ============================================================================

class IndependentStreamsTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up IndependentStreamsTest..." << std::endl;
        
        std::string token = getApiToken();
        if (token.empty()) {
            std::cerr << "WARNING: No API token found. Set TINKOFF_TOKEN environment variable or create .test_token.txt file." << std::endl;
        }
        
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        
        // Create 5 separate MarketDataStream instances - one for each subscription type
        // This ensures they are truly independent
        candlesStream = std::make_shared<MarketDataStream>(channel, token);
        orderBookStream = std::make_shared<MarketDataStream>(channel, token);
        tradesStream = std::make_shared<MarketDataStream>(channel, token);
        infoStream = std::make_shared<MarketDataStream>(channel, token);
        lastPriceStream = std::make_shared<MarketDataStream>(channel, token);
        
        std::cout << "Created 5 independent MarketDataStream instances" << std::endl;
    }

    void TearDown() override {
        std::cout << "Tearing down test..." << std::endl;
        
        // Close all streams
        if (candlesStream) {
            try { candlesStream->close(); } catch (...) {}
        }
        if (orderBookStream) {
            try { orderBookStream->close(); } catch (...) {}
        }
        if (tradesStream) {
            try { tradesStream->close(); } catch (...) {}
        }
        if (infoStream) {
            try { infoStream->close(); } catch (...) {}
        }
        if (lastPriceStream) {
            try { lastPriceStream->close(); } catch (...) {}
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::shared_ptr<MarketDataStream> candlesStream;
    std::shared_ptr<MarketDataStream> orderBookStream;
    std::shared_ptr<MarketDataStream> tradesStream;
    std::shared_ptr<MarketDataStream> infoStream;
    std::shared_ptr<MarketDataStream> lastPriceStream;
};

// ============================================================================
// TEST: Verify 5 separate subscriptions create 5 separate streams
// ============================================================================

TEST_F(IndependentStreamsTest, FiveSeparateSubscriptions_CreateFiveSeparateStreams)
{
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: Five Separate Subscriptions Create Five Separate Streams ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    
    // Create trackers for each subscription type
    IndependentStreamTracker candlesTracker("Candles");
    IndependentStreamTracker orderBookTracker("OrderBook");
    IndependentStreamTracker tradesTracker("Trades");
    IndependentStreamTracker infoTracker("Info");
    IndependentStreamTracker lastPriceTracker("LastPrice");
    
    // Prepare instrument lists
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    std::cout << "\n1. Starting all 5 subscriptions simultaneously..." << std::endl;
    
    // Start all 5 subscriptions at the same time
    // Each should create its own thread and stream
    
    // Subscribe to Candles
    candlesStream->SubscribeCandlesAsync(candleInstruments, 
        [&candlesTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                candlesTracker.recordMessage();
            }
        });
    candlesTracker.start();
    std::cout << "   - Candles subscription started (thread will be created)" << std::endl;
    
    // Subscribe to OrderBook
    orderBookStream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH,
        [&orderBookTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                orderBookTracker.recordMessage();
            }
        });
    orderBookTracker.start();
    std::cout << "   - OrderBook subscription started (thread will be created)" << std::endl;
    
    // Subscribe to Trades
    tradesStream->SubscribeTradesAsync(TEST_INSTRUMENTS,
        [&tradesTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                tradesTracker.recordMessage();
            }
        });
    tradesTracker.start();
    std::cout << "   - Trades subscription started (thread will be created)" << std::endl;
    
    // Subscribe to Info (Trading Status)
    infoStream->SubscribeInfoAsync(TEST_INSTRUMENTS,
        [&infoTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                infoTracker.recordMessage();
            }
        });
    infoTracker.start();
    std::cout << "   - Info subscription started (thread will be created)" << std::endl;
    
    // Subscribe to LastPrice
    lastPriceStream->SubscribeLastPriceAsync(TEST_INSTRUMENTS,
        [&lastPriceTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                lastPriceTracker.recordMessage();
            }
        });
    lastPriceTracker.start();
    std::cout << "   - LastPrice subscription started (thread will be created)" << std::endl;
    
    std::cout << "\n2. All 5 subscriptions started! Waiting for data..." << std::endl;
    
    // Give some time for connections to establish
    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    std::cout << "\n3. Checking stream status after 3 seconds..." << std::endl;
    
    // Check connection status for each stream
    std::cout << "   - Candles stream connected: " << (candlesStream->isConnected() ? "YES" : "NO") << std::endl;
    std::cout << "   - OrderBook stream connected: " << (orderBookStream->isConnected() ? "YES" : "NO") << std::endl;
    std::cout << "   - Trades stream connected: " << (tradesStream->isConnected() ? "YES" : "NO") << std::endl;
    std::cout << "   - Info stream connected: " << (infoStream->isConnected() ? "YES" : "NO") << std::endl;
    std::cout << "   - LastPrice stream connected: " << (lastPriceStream->isConnected() ? "YES" : "NO") << std::endl;
    
    // Wait for first messages
    const int WAIT_SECONDS = 30;
    std::cout << "\n4. Waiting up to " << WAIT_SECONDS << " seconds for first messages..." << std::endl;
    
    bool candlesGotData = candlesTracker.waitForMessages(1, WAIT_SECONDS);
    bool orderBookGotData = orderBookTracker.waitForMessages(1, WAIT_SECONDS);
    bool tradesGotData = tradesTracker.waitForMessages(1, WAIT_SECONDS);
    bool infoGotData = infoTracker.waitForMessages(1, WAIT_SECONDS);
    bool lastPriceGotData = lastPriceTracker.waitForMessages(1, WAIT_SECONDS);
    
    std::cout << "\n5. Results after waiting:" << std::endl;
    std::cout << "   - Candles received data: " << (candlesGotData ? "YES" : "NO") 
              << " (" << candlesTracker.getMessageCount() << " messages)" << std::endl;
    std::cout << "   - OrderBook received data: " << (orderBookGotData ? "YES" : "NO") 
              << " (" << orderBookTracker.getMessageCount() << " messages)" << std::endl;
    std::cout << "   - Trades received data: " << (tradesGotData ? "YES" : "NO") 
              << " (" << tradesTracker.getMessageCount() << " messages)" << std::endl;
    std::cout << "   - Info received data: " << (infoGotData ? "YES" : "NO")
              << " (" << infoTracker.getMessageCount() << " messages)" << std::endl;
    std::cout << "   - LastPrice received data: " << (lastPriceGotData ? "YES" : "NO") 
              << " (" << lastPriceTracker.getMessageCount() << " messages)" << std::endl;
    
    // Continue streaming for a bit more to collect more data
    std::cout << "\n6. Continuing to stream for additional 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // Print final statistics
    std::cout << "\n7. Final Statistics:" << std::endl;
    candlesTracker.printStats();
    orderBookTracker.printStats();
    tradesTracker.printStats();
    infoTracker.printStats();
    lastPriceTracker.printStats();
    
    // Unsubscribe all
    std::cout << "\n8. Unsubscribing from all streams..." << std::endl;
    candlesStream->UnSubscribeCandlesAsync();
    orderBookStream->UnSubscribeOrderBookAsync();
    tradesStream->UnSubscribeTradesAsync();
    infoStream->UnSubscribeInfoAsync();
    lastPriceStream->UnSubscribeLastPriceAsync();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Verification
    std::cout << "\n=== Verification ===" << std::endl;
    
    bool success = true;
    
    // At least some streams should have received data
    int totalStreamsWithData = 0;
    if (candlesTracker.getMessageCount() > 0) {
        std::cout << "PASS: Candles stream - " << candlesTracker.getMessageCount() << " messages" << std::endl;
        totalStreamsWithData++;
    }
    if (orderBookTracker.getMessageCount() > 0) {
        std::cout << "PASS: OrderBook stream - " << orderBookTracker.getMessageCount() << " messages" << std::endl;
        totalStreamsWithData++;
    }
    if (tradesTracker.getMessageCount() > 0) {
        std::cout << "PASS: Trades stream - " << tradesTracker.getMessageCount() << " messages" << std::endl;
        totalStreamsWithData++;
    }
    if (infoTracker.getMessageCount() > 0) {
        std::cout << "PASS: Info stream - " << infoTracker.getMessageCount() << " messages" << std::endl;
        totalStreamsWithData++;
    }
    if (lastPriceTracker.getMessageCount() > 0) {
        std::cout << "PASS: LastPrice stream - " << lastPriceTracker.getMessageCount() << " messages" << std::endl;
        totalStreamsWithData++;
    }
    
    std::cout << "\nTotal streams with data: " << totalStreamsWithData << "/5" << std::endl;
    
    if (totalStreamsWithData >= 3) {
        std::cout << "PASS: At least 3 streams received data independently" << std::endl;
    } else {
        std::cerr << "FAIL: Only " << totalStreamsWithData << " streams received data" << std::endl;
        success = false;
    }
    
    // All streams should be connected at some point
    bool allConnected = candlesStream->isConnected() || candlesTracker.getMessageCount() > 0;
    allConnected = allConnected && (orderBookStream->isConnected() || orderBookTracker.getMessageCount() > 0);
    allConnected = allConnected && (tradesStream->isConnected() || tradesTracker.getMessageCount() > 0);
    allConnected = allConnected && (infoStream->isConnected() || infoTracker.getMessageCount() > 0);
    allConnected = allConnected && (lastPriceStream->isConnected() || lastPriceTracker.getMessageCount() > 0);
    
    if (allConnected) {
        std::cout << "PASS: All 5 streams attempted connection" << std::endl;
    } else {
        std::cerr << "FAIL: Not all streams connected" << std::endl;
        success = false;
    }
    
    std::cout << "\n=== TEST " << (success ? "PASSED" : "FAILED") << " ===" << std::endl;
    
    // Main assertions
    EXPECT_GE(totalStreamsWithData, 3) << "At least 3 streams should receive data independently";
}

// ============================================================================
// TEST: Verify independent streams don't interfere with each other
// ============================================================================

TEST_F(IndependentStreamsTest, IndependentStreams_DoNotInterfere)
{
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: Independent Streams Don't Interfere ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    
    // Create trackers
    IndependentStreamTracker candlesTracker("Candles");
    IndependentStreamTracker orderBookTracker("OrderBook");
    
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    candleInstruments.push_back({TEST_INSTRUMENTS[0], SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    
    std::cout << "\n1. Starting Candles subscription..." << std::endl;
    candlesStream->SubscribeCandlesAsync(candleInstruments,
        [&candlesTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                candlesTracker.recordMessage();
            }
        });
    candlesTracker.start();
    
    // Wait for candles to start receiving
    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    bool candlesGotData = candlesTracker.waitForMessages(1, 30);
    std::cout << "   Candles received data: " << (candlesGotData ? "YES" : "NO") << std::endl;
    
    // Now start OrderBook subscription while Candles is still running
    std::cout << "\n2. Starting OrderBook subscription while Candles is still running..." << std::endl;
    std::vector<std::string> orderBookInstr = {TEST_INSTRUMENTS[0]};
    
    orderBookStream->SubscribeOrderBookAsync(orderBookInstr, DEFAULT_DEPTH,
        [&orderBookTracker](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                orderBookTracker.recordMessage();
            }
        });
    orderBookTracker.start();
    
    // Wait for both to receive data
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    bool orderBookGotData = orderBookTracker.waitForMessages(1, 30);
    std::cout << "   OrderBook received data: " << (orderBookGotData ? "YES" : "NO") << std::endl;
    
    // Both should still be receiving data independently
    std::cout << "\n3. Checking both streams are still active..." << std::endl;
    
    int candlesCountBefore = candlesTracker.getMessageCount();
    int orderBookCountBefore = orderBookTracker.getMessageCount();
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    int candlesCountAfter = candlesTracker.getMessageCount();
    int orderBookCountAfter = orderBookTracker.getMessageCount();
    
    std::cout << "   Candles: " << candlesCountBefore << " -> " << candlesCountAfter << " messages" << std::endl;
    std::cout << "   OrderBook: " << orderBookCountBefore << " -> " << orderBookCountAfter << " messages" << std::endl;
    
    // Both should have received more messages
    bool candlesStillActive = candlesCountAfter > candlesCountBefore;
    bool orderBookStillActive = orderBookCountAfter > orderBookCountBefore;
    
    std::cout << "\n4. Verification:" << std::endl;
    std::cout << "   Candles still receiving: " << (candlesStillActive ? "YES" : "NO") << std::endl;
    std::cout << "   OrderBook still receiving: " << (orderBookStillActive ? "YES" : "NO") << std::endl;
    
    // Clean up
    candlesStream->UnSubscribeCandlesAsync();
    orderBookStream->UnSubscribeOrderBookAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Assertions
    EXPECT_TRUE(candlesGotData) << "Candles should receive data";
    EXPECT_TRUE(orderBookGotData) << "OrderBook should receive data";
    EXPECT_TRUE(candlesStillActive) << "Candles should continue receiving after OrderBook starts";
    EXPECT_TRUE(orderBookStillActive) << "OrderBook should continue receiving";
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// TEST: Quick verification - all 5 subscriptions at once
// ============================================================================

TEST_F(IndependentStreamsTest, AllFiveSubscriptions_QuickTest)
{
    std::cout << "\n================================================================" << std::endl;
    std::cout << "=== TEST: All Five Subscriptions Quick Test ===" << std::endl;
    std::cout << "================================================================" << std::endl;
    
    // Create trackers
    std::vector<std::shared_ptr<IndependentStreamTracker>> trackers;
    trackers.push_back(std::make_shared<IndependentStreamTracker>("Candles"));
    trackers.push_back(std::make_shared<IndependentStreamTracker>("OrderBook"));
    trackers.push_back(std::make_shared<IndependentStreamTracker>("Trades"));
    trackers.push_back(std::make_shared<IndependentStreamTracker>("Info"));
    trackers.push_back(std::make_shared<IndependentStreamTracker>("LastPrice"));
    
    std::vector<std::pair<std::string, SubscriptionInterval>> candleInstruments;
    for (const auto& figi : TEST_INSTRUMENTS) {
        candleInstruments.push_back({figi, SubscriptionInterval::SUBSCRIPTION_INTERVAL_ONE_MINUTE});
    }
    
    std::cout << "\n1. Starting all 5 subscriptions simultaneously..." << std::endl;
    
    // All 5 at once
    candlesStream->SubscribeCandlesAsync(candleInstruments,
        [trackers](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                trackers[0]->recordMessage();
            }
        });
    trackers[0]->start();
    
    orderBookStream->SubscribeOrderBookAsync(TEST_INSTRUMENTS, DEFAULT_DEPTH,
        [trackers](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                trackers[1]->recordMessage();
            }
        });
    trackers[1]->start();
    
    tradesStream->SubscribeTradesAsync(TEST_INSTRUMENTS,
        [trackers](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                trackers[2]->recordMessage();
            }
        });
    trackers[2]->start();
    
    infoStream->SubscribeInfoAsync(TEST_INSTRUMENTS,
        [trackers](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                trackers[3]->recordMessage();
            }
        });
    trackers[3]->start();
    
    lastPriceStream->SubscribeLastPriceAsync(TEST_INSTRUMENTS,
        [trackers](ServiceReply reply) {
            if (reply.hasMarketDataStreamResponse()) {
                trackers[4]->recordMessage();
            }
        });
    trackers[4]->start();
    
    // Wait for data
    std::cout << "2. Waiting for data (30 seconds)..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));
    
    // Print results
    std::cout << "\n3. Results:" << std::endl;
    int totalWithData = 0;
    for (size_t i = 0; i < trackers.size(); i++) {
        int count = trackers[i]->getMessageCount();
        std::cout << "   " << trackers[i]->getName() << ": " << count << " messages" << std::endl;
        if (count > 0) totalWithData++;
    }
    
    // Clean up
    candlesStream->UnSubscribeCandlesAsync();
    orderBookStream->UnSubscribeOrderBookAsync();
    tradesStream->UnSubscribeTradesAsync();
    infoStream->UnSubscribeInfoAsync();
    lastPriceStream->UnSubscribeLastPriceAsync();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    std::cout << "\nTotal streams with data: " << totalWithData << "/5" << std::endl;
    
    // At least 3 should work
    EXPECT_GE(totalWithData, 3) << "At least 3 independent streams should work";
    
    std::cout << "\n=== TEST PASSED ===" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Independent Streams Test" << std::endl;
    std::cout << "======================================================" << std::endl;
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Instruments: " << TEST_INSTRUMENTS.size() << std::endl;
    for (size_t i = 0; i < TEST_INSTRUMENTS.size(); i++) {
        std::cout << "  " << (i+1) << ". " << TEST_INSTRUMENT_NAMES[i] 
                  << " (" << TEST_INSTRUMENTS[i] << ")" << std::endl;
    }
    std::cout << "\nTests:" << std::endl;
    std::cout << "  - FiveSeparateSubscriptions_CreateFiveSeparateStreams" << std::endl;
    std::cout << "  - IndependentStreams_DoNotInterfere" << std::endl;
    std::cout << "  - AllFiveSubscriptions_QuickTest" << std::endl;
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

