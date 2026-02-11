/**
 * @file test_streaming_instrument_id.cpp
 * @brief Streaming API Tests for InstrumentId and FigiOld Methods
 * 
 * This file contains tests for:
 * - New MarketDataStream methods using instrument_id
 * - Legacy MarketDataStream methods with FigiOld suffix (backward compatibility)
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
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

const std::string TEST_TOKEN = "test_token";
const std::string TEST_HOST = "localhost:50051";
const std::string TEST_FIGI = "BBG000B9XRY4";
const std::string TEST_INSTRUMENT_ID = "TCS-001S-FF";

// ============================================================================
// MarketDataStream InstrumentId Tests (New API)
// ============================================================================

class MarketDataStreamInstrumentIdTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
};

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeCandlesWithInstrumentId) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Test new method using instrument_id
    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_INSTRUMENT_ID, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandles(instruments, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeCandlesAsyncWithInstrumentId) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_INSTRUMENT_ID, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(instruments, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeOrderBookWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // New method takes vector of instrument IDs
    std::vector<std::string> instrumentIds = {
        TEST_INSTRUMENT_ID,
        "TCS-002S-FF",
        "SBER-001S-FF"
    };

    EXPECT_NO_THROW({
        stream->SubscribeOrderBook(instrumentIds, 10, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeOrderBookAsyncWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {TEST_INSTRUMENT_ID};

    EXPECT_NO_THROW({
        stream->SubscribeOrderBookAsync(instrumentIds, 20, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeTradesWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {
        TEST_INSTRUMENT_ID,
        "GAZP-001S-FF"
    };

    EXPECT_NO_THROW({
        stream->SubscribeTrades(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeTradesAsyncWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {TEST_INSTRUMENT_ID};

    EXPECT_NO_THROW({
        stream->SubscribeTradesAsync(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeInfoWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {
        TEST_INSTRUMENT_ID,
        "YNDX-001S-FF"
    };

    EXPECT_NO_THROW({
        stream->SubscribeInfo(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeInfoAsyncWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {TEST_INSTRUMENT_ID};

    EXPECT_NO_THROW({
        stream->SubscribeInfoAsync(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeLastPriceWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {
        TEST_INSTRUMENT_ID,
        "SBER-001S-FF",
        "VTBR-001S-FF"
    };

    EXPECT_NO_THROW({
        stream->SubscribeLastPrice(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, SubscribeLastPriceAsyncWithInstrumentIds) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> instrumentIds = {TEST_INSTRUMENT_ID};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(instrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, MultipleInstrumentIdsSubscription) {
    std::atomic<int> candleCount{0};
    std::atomic<int> tradeCount{0};

    auto candleCallback = [&candleCount](ServiceReply reply) {
        candleCount++;
    };

    auto tradeCallback = [&tradeCount](ServiceReply reply) {
        tradeCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {TEST_INSTRUMENT_ID, SUBSCRIPTION_INTERVAL_ONE_MINUTE},
        {"SBER-001S-FF", SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    std::vector<std::string> trades = {TEST_INSTRUMENT_ID, "GAZP-001S-FF"};

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(candles, candleCallback);
        stream->SubscribeTradesAsync(trades, tradeCallback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, EmptyInstrumentIdListHandlesGracefully) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> emptyInstrumentIds = {};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceAsync(emptyInstrumentIds, callback);
    });
}

TEST_F(MarketDataStreamInstrumentIdTest, AllSubscriptionIntervalsWithInstrumentId) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Test all available subscription intervals
    std::vector<SubscriptionInterval> intervals = {
        SUBSCRIPTION_INTERVAL_ONE_MINUTE,
        SUBSCRIPTION_INTERVAL_FIVE_MINUTES
    };

    for (auto interval : intervals) {
        std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
            {TEST_INSTRUMENT_ID, interval}
        };

        EXPECT_NO_THROW({
            stream->SubscribeCandlesAsync(instruments, callback);
        });
    }
}

// ============================================================================
// MarketDataStream FigiOld Tests (Legacy API for backward compatibility)
// ============================================================================

class MarketDataStreamFigiOldTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
    const std::string TEST_FIGI_LEGACY = "BBG000B9XRY4";
};

TEST_F(MarketDataStreamFigiOldTest, SubscribeCandlesFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Legacy method using figi
    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI_LEGACY, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandlesFigiOld(instruments, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeCandlesFigiOldAsync) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_FIGI_LEGACY, SUBSCRIPTION_INTERVAL_FIVE_MINUTES}
    };

    EXPECT_NO_THROW({
        stream->SubscribeCandlesFigiOldAsync(instruments, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeOrderBookFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Legacy method with single figi parameter (blocking version)
    EXPECT_NO_THROW({
        stream->SubscribeOrderBookFigiOld(TEST_FIGI_LEGACY, 10, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeOrderBookFigiOldAsync) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Legacy async method with vector of figis
    std::vector<std::string> figis = {TEST_FIGI_LEGACY};

    EXPECT_NO_THROW({
        stream->SubscribeOrderBookFigiOldAsync(figis, 20, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeTradesFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {
        TEST_FIGI_LEGACY,
        "BBG004730JJ5"
    };

    EXPECT_NO_THROW({
        stream->SubscribeTradesFigiOld(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeTradesFigiOldAsync) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI_LEGACY};

    EXPECT_NO_THROW({
        stream->SubscribeTradesFigiOldAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeInfoFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {
        TEST_FIGI_LEGACY,
        "BBG004730JJ5"
    };

    EXPECT_NO_THROW({
        stream->SubscribeInfoFigiOld(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeInfoFigiOldAsync) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI_LEGACY};

    EXPECT_NO_THROW({
        stream->SubscribeInfoFigiOldAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeLastPriceFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {
        TEST_FIGI_LEGACY,
        "BBG004730JJ5",
        "BBG00JXPFBN0"
    };

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceFigiOld(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, SubscribeLastPriceFigiOldAsync) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> figis = {TEST_FIGI_LEGACY};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceFigiOldAsync(figis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, MultipleFigiOldSubscriptions) {
    std::atomic<int> candleCount{0};
    std::atomic<int> tradeCount{0};

    auto candleCallback = [&candleCount](ServiceReply reply) {
        candleCount++;
    };

    auto tradeCallback = [&tradeCount](ServiceReply reply) {
        tradeCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {TEST_FIGI_LEGACY, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    std::vector<std::string> trades = {TEST_FIGI_LEGACY};

    EXPECT_NO_THROW({
        stream->SubscribeCandlesFigiOldAsync(candles, candleCallback);
        stream->SubscribeTradesFigiOldAsync(trades, tradeCallback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, EmptyFigiOldListHandlesGracefully) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    std::vector<std::string> emptyFigis = {};

    EXPECT_NO_THROW({
        stream->SubscribeLastPriceFigiOldAsync(emptyFigis, callback);
    });
}

TEST_F(MarketDataStreamFigiOldTest, AllSubscriptionIntervalsWithFigiOld) {
    std::atomic<int> messageCount{0};
    auto callback = [&messageCount](ServiceReply reply) {
        messageCount++;
    };

    // Test all available subscription intervals with legacy API
    std::vector<SubscriptionInterval> intervals = {
        SUBSCRIPTION_INTERVAL_ONE_MINUTE,
        SUBSCRIPTION_INTERVAL_FIVE_MINUTES
    };

    for (auto interval : intervals) {
        std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
            {TEST_FIGI_LEGACY, interval}
        };

        EXPECT_NO_THROW({
            stream->SubscribeCandlesFigiOldAsync(instruments, callback);
        });
    }
}

// ============================================================================
// MarketDataStream API Comparison Tests
// ============================================================================

class MarketDataStreamApiComparisonTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        stream = std::make_shared<MarketDataStream>(channel, TEST_TOKEN);
    }

    std::shared_ptr<MarketDataStream> stream;
    const std::string TEST_INSTRUMENT_ID = "TCS-001S-FF";
    const std::string TEST_FIGI = "BBG000B9XRY4";
};

TEST_F(MarketDataStreamApiComparisonTest, BothApisCompileAndRun) {
    std::atomic<int> instrumentIdCount{0};
    std::atomic<int> figiOldCount{0};

    auto instrumentIdCallback = [&instrumentIdCount](ServiceReply reply) {
        instrumentIdCount++;
    };

    auto figiOldCallback = [&figiOldCount](ServiceReply reply) {
        figiOldCount++;
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> instruments = {
        {TEST_INSTRUMENT_ID, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    std::vector<std::pair<std::string, SubscriptionInterval>> figiInstruments = {
        {TEST_FIGI, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    // Both APIs should work without throwing
    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(instruments, instrumentIdCallback);
        stream->SubscribeCandlesFigiOldAsync(figiInstruments, figiOldCallback);
    });
}

TEST_F(MarketDataStreamApiComparisonTest, SubscribeOrderBookSignatureDifference) {
    std::atomic<int> instrumentIdCount{0};
    std::atomic<int> figiOldCount{0};

    auto instrumentIdCallback = [&instrumentIdCount](ServiceReply reply) {
        instrumentIdCount++;
    };

    auto figiOldCallback = [&figiOldCount](ServiceReply reply) {
        figiOldCount++;
    };

    // New API: vector of instrumentIds
    std::vector<std::string> instrumentIds = {TEST_INSTRUMENT_ID};

    // Legacy API: vector of figis for async
    std::vector<std::string> figis = {TEST_FIGI};

    EXPECT_NO_THROW({
        stream->SubscribeOrderBookAsync(instrumentIds, 10, instrumentIdCallback);
        stream->SubscribeOrderBookFigiOldAsync(figis, 10, figiOldCallback);
    });
}

TEST_F(MarketDataStreamApiComparisonTest, MixedSubscriptionsWork) {
    std::atomic<int> instrumentIdCount{0};
    std::atomic<int> figiOldCount{0};

    auto instrumentIdCallback = [&instrumentIdCount](ServiceReply reply) {
        instrumentIdCount++;
    };

    auto figiOldCallback = [&figiOldCount](ServiceReply reply) {
        figiOldCount++;
    };

    // Mix new API and legacy API subscriptions
    std::vector<std::pair<std::string, SubscriptionInterval>> candles = {
        {TEST_INSTRUMENT_ID, SUBSCRIPTION_INTERVAL_ONE_MINUTE}
    };

    std::vector<std::string> figiTrades = {TEST_FIGI};
    std::vector<std::string> instrumentIdTrades = {TEST_INSTRUMENT_ID};

    EXPECT_NO_THROW({
        stream->SubscribeCandlesAsync(candles, instrumentIdCallback);
        stream->SubscribeTradesFigiOldAsync(figiTrades, figiOldCallback);
        stream->SubscribeTradesAsync(instrumentIdTrades, instrumentIdCallback);
    });
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

