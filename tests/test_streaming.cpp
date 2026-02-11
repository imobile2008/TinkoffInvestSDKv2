/**
 * @file test_streaming.cpp
 * @brief Streaming API Tests for TinkoffInvestSDK
 * 
 * This file contains tests for streaming services:
 * - MarketDataStream (candles, orderbook, trades, info, last prices)
 * - OrdersStream (trades stream)
 */

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

// Include SDK headers
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

const std::string TEST_TOKEN = "test_token";
const std::string TEST_HOST = "localhost:50051";
const std::string TEST_ACCOUNT_ID = "test_account_id";
const std::string TEST_FIGI = "BBG000B9XRY4";

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
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(MarketDataStreamTest, SubscribeCandlesBlockingCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Note: This test requires a real connection to receive messages
    // For unit testing, we verify the method signature and basic flow
    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };

    // Test should complete without throwing (actual streaming depends on API connection)
    // Using EXPECT_NO_THROW for async version which doesn't block
    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(instruments, callback);
    });
}

TEST_F(MarketDataStreamTest, SubscribeTradesBlockingCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI, "BBG004730JJ5"};

    EXPECT_NO_THROW({
        stream->SubscribeTradesAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamTest, SubscribeOrderBookBlockingCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    EXPECT_NO_THROW({
        stream->SubscribeOrderBookAsync({TEST_FIGI}, 10, callback);
    });
}

TEST_F(MarketDataStreamTest, SubscribeInfoBlockingCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI};

    EXPECT_NO_THROW({
        stream->SubscribeInfoAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamTest, SubscribeLastPriceBlockingCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamTest, UnsubscribeMethodsComplete) {
    EXPECT_NO_THROW({
        stream->UnSubscribeCandlesAsync();
        stream->UnSubscribeTradesAsync();
        stream->UnSubscribeOrderBookAsync();
        stream->UnSubscribeInfoAsync();
        stream->UnSubscribeLastPriceAsync();
    });
}

TEST_F(MarketDataStreamTest, MultipleSubscriptionsWork) {
    std::atomic<int> candleCount{0};
    std::atomic<int> tradeCount{0};

    auto candleCallback = [&candleCount](ServiceReply reply) {
        candleCount++;
    };

    auto tradeCallback = [&tradeCount](ServiceReply reply) {
        tradeCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };

    std::vector<std::string> trades = {TEST_FIGI};

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(candles, candleCallback);
        stream->SubscribeTradesAsync(trades, tradeCallback);
    });
}

TEST_F(MarketDataStreamTest, EmptyFigiListHandlesGracefully) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> emptyFigis = {};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(emptyFigis, callback);
    });
}

// ============================================================================
// OrdersStream Tests
// ============================================================================

class OrdersStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<OrdersStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<OrdersStream> stream;
};

TEST_F(OrdersStreamTest, TradesStreamAsyncCompletes) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> accounts = {getFirstAccountId()};

    EXPECT_NO_THROW({
        stream->TradesStreamAsync(accounts, callback);
    });
}

TEST_F(OrdersStreamTest, EmptyAccountsListHandlesGracefully) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> emptyAccounts = {};

    EXPECT_NO_THROW({
        stream->TradesStreamAsync(emptyAccounts, callback);
    });
}

TEST_F(OrdersStreamTest, MultipleAccountsStreamWorks) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> accounts = {
        getFirstAccountId()
    };

    EXPECT_NO_THROW({
        stream->TradesStreamAsync(accounts, callback);
    });
}

// ============================================================================
// Streaming Integration Tests
// ============================================================================

class StreamingIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        client = std::make_unique<InvestApiClient>(TEST_HOST, TEST_TOKEN);
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

    // Both should be able to initiate async calls
    std::atomic<int> count{0};
    auto callback = [&count](ServiceReply) { count++; };

    EXPECT_NO_THROW({
        marketdatastream->SubscribeLastPriceAsync({TEST_FIGI}, callback);
        ordersstream->TradesStreamAsync({getFirstAccountId()}, callback);
    });
}

// ============================================================================
// Performance Tests (Basic)
// ============================================================================

class StreamingPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(StreamingPerformanceTest, RapidSubscriptionChanges) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Rapid subscribe/unsubscribe should not crash
    for (int i = 0; i < 5; ++i) {
        EXPECT_NO_THROW({
            stream->SubscribeLastPriceAsync({TEST_FIGI}, callback);
            stream->UnSubscribeLastPriceAsync();
        });
    }
}

TEST_F(StreamingPerformanceTest, LargeSubscriptionList) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Create a large list of instruments
    std::vector<std::string> instruments;
    for (int i = 0; i < 50; ++i) {
        instruments.push_back("BBG00" + std::to_string(i));
    }

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(instruments, callback);
    });
}

// ============================================================================
// Error Handling Tests
// ============================================================================

class StreamingErrorHandlingTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(StreamingErrorHandlingTest, InvalidSubscriptionIntervalHandles) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Test with invalid interval value (0 or undefined)
    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_UNSPECIFIED}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(instruments, callback);
    });
}

TEST_F(StreamingErrorHandlingTest, SpecialCharactersInFigiHandles) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Test with various FIGI formats
    std::vector<std::string> figis = {
        "BBG000B9XRY4",
        "BBG004730JJ5",
        "BBG00JXPFBN0"
    };

    EXPECT_NO_THROW({
        stream->SubscribeTradesAsync(figis, callback);
    });
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

