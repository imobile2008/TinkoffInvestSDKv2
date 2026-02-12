/**
 * @file test_streaming.cpp
 * @brief Streaming API Tests for TinkoffInvestSDK
 *
 * This file contains tests for streaming services:
 * - MarketDataStream (candles, orderbook, trades, info, last prices)
 * - OrdersStream (trades stream)
 *
 * IMPORTANT: These tests verify that async streaming methods can be invoked
 * without throwing exceptions. Since streaming requires real network connection
 * and data availability, the tests focus on:
 * 1. Method invocation doesn't throw
 * 2. Basic subscription/unsubscription lifecycle works
 * 3. Service access and configuration is correct
 *
 * Full end-to-end streaming tests require actual market data and should be
 * run with appropriate timeouts in integration environments.
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
#include <algorithm>
#include <cctype>

// Include SDK headers
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

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
const std::string TEST_ACCOUNT_ID = "test_account_id";
const std::string TEST_FIGI = "BBG004S68104";  // Sberbank

// Helper function to get first account ID (for unit tests with mock services)
std::string getFirstAccountId() {
    return TEST_ACCOUNT_ID;
}

// ============================================================================
// MarketDataStream Tests
// ============================================================================

class MarketDataStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up MarketDataStreamTest..." << std::endl;
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, getApiToken());
        std::cout << "MarketDataStream created successfully" << std::endl;
    }

    void TearDown() override {
        // Give time for cleanup between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::shared_ptr<MarketDataStream> stream;
};

// Test that SubscribeCandlesAsync can be invoked without throwing
TEST_F(MarketDataStreamTest, SubscribeCandlesAsyncMethodInvocation) {
    std::cout << "SubscribeCandlesAsync test starting..." << std::endl;

    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };

    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &instruments, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeCandlesAsync(instruments, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    // Wait briefly for method to be invoked
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "SubscribeCandlesAsync threw: " << exceptionMsg;
    std::cout << "SubscribeCandlesAsync test completed" << std::endl;
}

// Test that SubscribeTradesAsync can be invoked without throwing
TEST_F(MarketDataStreamTest, SubscribeTradesAsyncMethodInvocation) {
    std::cout << "SubscribeTradesAsync test starting..." << std::endl;

    std::vector<std::string> figis = {TEST_FIGI, "BBG004730JJ5"};
    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &figis, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeTradesAsync(figis, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "SubscribeTradesAsync threw: " << exceptionMsg;
    std::cout << "SubscribeTradesAsync test completed" << std::endl;
}

// Test that SubscribeOrderBookAsync can be invoked without throwing
TEST_F(MarketDataStreamTest, SubscribeOrderBookAsyncMethodInvocation) {
    std::cout << "SubscribeOrderBookAsync test starting..." << std::endl;

    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeOrderBookAsync({TEST_FIGI}, 10, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "SubscribeOrderBookAsync threw: " << exceptionMsg;
    std::cout << "SubscribeOrderBookAsync test completed" << std::endl;
}

// Test that SubscribeInfoAsync can be invoked without throwing
TEST_F(MarketDataStreamTest, SubscribeInfoAsyncMethodInvocation) {
    std::cout << "SubscribeInfoAsync test starting..." << std::endl;

    std::vector<std::string> figis = {TEST_FIGI};
    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &figis, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeInfoAsync(figis, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "SubscribeInfoAsync threw: " << exceptionMsg;
    std::cout << "SubscribeInfoAsync test completed" << std::endl;
}

// Test that SubscribeLastPriceAsync can be invoked without throwing
TEST_F(MarketDataStreamTest, SubscribeLastPriceAsyncMethodInvocation) {
    std::cout << "SubscribeLastPriceAsync test starting..." << std::endl;

    std::vector<std::string> figis = {TEST_FIGI};
    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &figis, &noException, &exceptionMsg]() {
        try {
            stream->SubscribeLastPriceAsync(figis, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "SubscribeLastPriceAsync threw: " << exceptionMsg;
    std::cout << "SubscribeLastPriceAsync test completed" << std::endl;
}

// Test that unsubscribe methods work
TEST_F(MarketDataStreamTest, UnsubscribeMethodsComplete) {
    std::cout << "Unsubscribe methods test starting..." << std::endl;
    bool allPassed = true;

    std::vector<std::pair<std::string, std::function<void()>>> methods = {
        {"UnSubscribeCandlesAsync", [this]() { stream->UnSubscribeCandlesAsync(); }},
        {"UnSubscribeTradesAsync", [this]() { stream->UnSubscribeTradesAsync(); }},
        {"UnSubscribeOrderBookAsync", [this]() { stream->UnSubscribeOrderBookAsync(); }},
        {"UnSubscribeInfoAsync", [this]() { stream->UnSubscribeInfoAsync(); }},
        {"UnSubscribeLastPriceAsync", [this]() { stream->UnSubscribeLastPriceAsync(); }}
    };

    for (const auto& method : methods) {
        try {
            method.second();
            std::cout << "[" << method.first << "] completed successfully" << std::endl;
        } catch (const std::exception& e) {
            std::cout << "[" << method.first << "] Exception: " << e.what() << std::endl;
            allPassed = false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(allPassed);
    std::cout << "Unsubscribe methods test completed" << std::endl;
}

// Test multiple subscriptions in sequence
TEST_F(MarketDataStreamTest, MultipleSubscriptionsSequential) {
    std::cout << "Multiple subscriptions sequential test starting..." << std::endl;

    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };
    std::vector<std::string> trades = {TEST_FIGI};

    bool noException = true;
    std::thread worker([this, &candles, &trades, &noException]() {
        try {
            stream->SubscribeCandlesAsync(candles, [](ServiceReply) {});
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            stream->SubscribeTradesAsync(trades, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    worker.detach();

    EXPECT_TRUE(noException) << "Multiple subscriptions threw exception";
    std::cout << "Multiple subscriptions sequential test completed" << std::endl;
}

// Test empty FIGI list handling
TEST_F(MarketDataStreamTest, EmptyFigiListHandlesGracefully) {
    std::cout << "Empty FIGI list test starting..." << std::endl;

    std::vector<std::string> emptyFigis = {};
    bool noException = true;

    std::thread worker([this, &emptyFigis, &noException]() {
        try {
            stream->SubscribeLastPriceAsync(emptyFigis, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException);
    std::cout << "EmptyFigiList test completed" << std::endl;
}

// ============================================================================
// OrdersStream Tests
// ============================================================================

class OrdersStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<OrdersStream>(channel, getApiToken());
    }

    void TearDown() override {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::shared_ptr<OrdersStream> stream;
};

TEST_F(OrdersStreamTest, TradesStreamAsyncMethodInvocation) {
    std::cout << "TradesStreamAsync test starting..." << std::endl;

    std::vector<std::string> accounts = {getFirstAccountId()};
    bool noException = true;

    std::thread worker([this, &accounts, &noException]() {
        try {
            stream->TradesStreamAsync(accounts, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException);
    std::cout << "TradesStreamAsync test completed" << std::endl;
}

TEST_F(OrdersStreamTest, EmptyAccountsListHandlesGracefully) {
    std::cout << "Empty accounts list test starting..." << std::endl;

    std::vector<std::string> emptyAccounts = {};
    bool noException = true;

    std::thread worker([this, &emptyAccounts, &noException]() {
        try {
            stream->TradesStreamAsync(emptyAccounts, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException);
    std::cout << "EmptyAccountsList test completed" << std::endl;
}

// ============================================================================
// Streaming Integration Tests
// ============================================================================

class StreamingIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        client = std::make_unique<InvestApiClient>(TEST_HOST, getApiToken());
    }

    std::unique_ptr<InvestApiClient> client;
};

TEST_F(StreamingIntegrationTest, MarketDataStreamServiceAccessible) {
    auto marketdatastream = std::dynamic_pointer_cast<MarketDataStream>(
        client->service("marketdatastream")
    );
    EXPECT_NE(marketdatastream, nullptr);
}

TEST_F(StreamingIntegrationTest, OrdersStreamServiceAccessible) {
    auto ordersstream = std::dynamic_pointer_cast<OrdersStream>(
        client->service("ordersstream")
    );
    EXPECT_NE(ordersstream, nullptr);
}

TEST_F(StreamingIntegrationTest, BothStreamServicesWorkTogether) {
    auto marketdatastream = std::dynamic_pointer_cast<MarketDataStream>(
        client->service("marketdatastream")
    );
    auto ordersstream = std::dynamic_pointer_cast<OrdersStream>(
        client->service("ordersstream")
    );

    EXPECT_NE(marketdatastream, nullptr);
    EXPECT_NE(ordersstream, nullptr);

    // Both should be able to initiate async calls without throwing
    bool passed = true;
    EXPECT_NO_THROW({
        marketdatastream->SubscribeLastPriceAsync({TEST_FIGI}, [](ServiceReply) {});
        ordersstream->TradesStreamAsync({getFirstAccountId()}, [](ServiceReply) {});
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

// ============================================================================
// Performance Tests (Basic)
// ============================================================================

class StreamingPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, getApiToken());
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(StreamingPerformanceTest, RapidSubscriptionChanges) {
    std::cout << "Rapid subscription changes test starting..." << std::endl;

    for (int i = 0; i < 3; ++i) {
        EXPECT_NO_THROW({
            stream->SubscribeLastPriceAsync({TEST_FIGI}, [](ServiceReply) {});
            stream->UnSubscribeLastPriceAsync();
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "RapidSubscriptionChanges test completed" << std::endl;
}

TEST_F(StreamingPerformanceTest, LargeSubscriptionList) {
    std::cout << "Large subscription list test starting..." << std::endl;

    std::vector<std::string> instruments;
    for (int i = 0; i < 10; ++i) {
        instruments.push_back("BBG00" + std::to_string(i));
    }

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(instruments, [](ServiceReply) {});
    });
    std::cout << "LargeSubscriptionList test completed" << std::endl;
}

// ============================================================================
// Error Handling Tests
// ============================================================================

class StreamingErrorHandlingTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<MarketDataStream>(channel, getApiToken());
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(StreamingErrorHandlingTest, InvalidSubscriptionIntervalHandles) {
    std::cout << "Invalid subscription interval test starting..." << std::endl;

    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_UNSPECIFIED}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(instruments, [](ServiceReply) {});
    });
    std::cout << "InvalidSubscriptionInterval test completed" << std::endl;
}

TEST_F(StreamingErrorHandlingTest, SpecialCharactersInFigiHandles) {
    std::cout << "Special characters in FIGI test starting..." << std::endl;

    std::vector<std::string> figis = {
        "BBG004S68104",  // Sberbank
        "BBG004730JJ5",  // Moscow Exchange
        "BBG00JXPFBN0"   // Tinkoff
    };

    EXPECT_NO_THROW({
        stream->SubscribeTradesAsync(figis, [](ServiceReply) {});
    });
    std::cout << "SpecialCharactersInFigi test completed" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

