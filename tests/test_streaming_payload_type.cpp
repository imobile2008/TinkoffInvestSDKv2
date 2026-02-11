/**
 * @file test_streaming_payload_type.cpp
 * @brief Comprehensive tests for MarketDataStreamResponse payload type handling
 * 
 * This test suite validates the new MarketDataStreamResponse wrapper class
 * and ensures proper payload type detection and type-safe access.
 */

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <stdexcept>

// Include SDK headers
#include "marketdatastreamresponse.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

const std::string TEST_HOST = "localhost:50051";

// ============================================================================
// MarketDataStreamResponse Basic Tests
// ============================================================================

class MarketDataStreamResponseTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MarketDataStreamResponseTest, DefaultConstructorCreatesUnknownPayload) {
    MarketDataStreamResponse response;
    EXPECT_EQ(response.getPayloadType(), MarketDataStreamResponse::PayloadType::UNKNOWN);
    EXPECT_FALSE(response.isSubscriptionConfirmation());
}

TEST_F(MarketDataStreamResponseTest, PayloadTypeToString) {
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::UNKNOWN), "UNKNOWN");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::TRADING_STATUS), "TRADING_STATUS");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE), "SUBSCRIBE_INFO_RESPONSE");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::CANDLE), "CANDLE");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::ORDERBOOK), "ORDERBOOK");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::TRADE), "TRADE");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::LAST_PRICE), "LAST_PRICE");
    EXPECT_EQ(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::PING), "PING");
}

TEST_F(MarketDataStreamResponseTest, GetPayloadTypeString) {
    MarketDataStreamResponse response;
    EXPECT_EQ(response.getPayloadTypeString(), "UNKNOWN");
}

// ============================================================================
// ServiceReply Integration Tests  
// ============================================================================

class ServiceReplyMarketDataStreamTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(ServiceReplyMarketDataStreamTest, DefaultServiceReplyHasNoStreamResponse) {
    ServiceReply reply;
    EXPECT_FALSE(reply.hasMarketDataStreamResponse());
}

TEST_F(ServiceReplyMarketDataStreamTest, ServiceReplyWithStreamResponse) {
    MarketDataStreamResponse streamResponse;
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.getMarketDataStreamResponse().getPayloadType(), 
              MarketDataStreamResponse::PayloadType::UNKNOWN);
}

TEST_F(ServiceReplyMarketDataStreamTest, ServiceReplyStreamResponseAccessor) {
    MarketDataStreamResponse streamResponse;
    ServiceReply reply(streamResponse);
    
    EXPECT_NO_THROW({
        const auto& response = reply.getMarketDataStreamResponse();
        EXPECT_EQ(response.getPayloadType(), MarketDataStreamResponse::PayloadType::UNKNOWN);
    });
}

TEST_F(ServiceReplyMarketDataStreamTest, ServiceReplyThrowsOnInvalidAccess) {
    ServiceReply reply;
    EXPECT_THROW(reply.getMarketDataStreamResponse(), std::runtime_error);
}

// ============================================================================
// Type-Safe Accessor Tests (Mock/Unit Tests - No gRPC Required)
// ============================================================================

class MarketDataStreamResponseTypeSafetyTest : public ::testing::Test {
protected:
    // Create a mock MarketDataResponse for testing type safety
    MarketDataResponse createTradingStatusResponse() {
        MarketDataResponse response;
        response.mutable_trading_status()->set_figi("BBG000B9XRY4");
        response.mutable_trading_status()->set_trading_status(SecurityTradingStatus::SECURITY_TRADING_STATUS_NORMAL_TRADING);
        return response;
    }
    
    MarketDataResponse createSubscribeInfoResponse() {
        MarketDataResponse response;
        response.mutable_subscribe_info_response()->set_tracking_id("test-tracking-id");
        // Note: SubscribeInfoResponse only contains tracking_id, not figi
        return response;
    }
};

TEST_F(MarketDataStreamResponseTypeSafetyTest, TradingStatusPayloadDetection) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse response(rawResponse);
    
    EXPECT_EQ(response.getPayloadType(), MarketDataStreamResponse::PayloadType::TRADING_STATUS);
    EXPECT_TRUE(response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS));
    EXPECT_FALSE(response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE));
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, SubscribeInfoPayloadDetection) {
    auto rawResponse = createSubscribeInfoResponse();
    MarketDataStreamResponse response(rawResponse);
    
    EXPECT_EQ(response.getPayloadType(), MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE);
    EXPECT_TRUE(response.isPayloadType(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE));
    EXPECT_FALSE(response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS));
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, TradingStatusTypeSafeAccessor) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse response(rawResponse);
    
    EXPECT_NO_THROW({
        const auto& tradingStatus = response.getTradingStatus();
        EXPECT_EQ(tradingStatus.figi(), "BBG000B9XRY4");
        EXPECT_EQ(tradingStatus.trading_status(), SecurityTradingStatus::SECURITY_TRADING_STATUS_NORMAL_TRADING);
    });
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, SubscribeInfoTypeSafeAccessor) {
    auto rawResponse = createSubscribeInfoResponse();
    MarketDataStreamResponse response(rawResponse);
    
    EXPECT_NO_THROW({
        const auto& subscribeInfo = response.getSubscribeInfoResponse();
        EXPECT_EQ(subscribeInfo.tracking_id(), "test-tracking-id");
        // Note: SubscribeInfoResponse only contains tracking_id, not figi
    });
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, WrongTypeAccessorThrows) {
    auto tradingStatusResponse = createTradingStatusResponse();
    MarketDataStreamResponse response(tradingStatusResponse);
    
    EXPECT_THROW(response.getSubscribeInfoResponse(), std::runtime_error);
    EXPECT_THROW(response.getCandle(), std::runtime_error);
    EXPECT_THROW(response.getOrderBook(), std::runtime_error);
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, IsSubscriptionConfirmation) {
    auto subscribeInfoResponse = createSubscribeInfoResponse();
    MarketDataStreamResponse response(subscribeInfoResponse);
    
    EXPECT_TRUE(response.isSubscriptionConfirmation());
    
    auto tradingStatusResponse = createTradingStatusResponse();
    MarketDataStreamResponse response2(tradingStatusResponse);
    
    EXPECT_FALSE(response2.isSubscriptionConfirmation());
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, GetTrackingId) {
    auto subscribeInfoResponse = createSubscribeInfoResponse();
    MarketDataStreamResponse response(subscribeInfoResponse);
    
    EXPECT_EQ(response.getTrackingId(), "test-tracking-id");
    
    auto tradingStatusResponse = createTradingStatusResponse();
    MarketDataStreamResponse response2(tradingStatusResponse);
    
    EXPECT_EQ(response2.getTrackingId(), ""); // Empty for non-subscribe info
}

TEST_F(MarketDataStreamResponseTypeSafetyTest, DebugString) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse response(rawResponse);
    
    EXPECT_NO_THROW({
        std::string debugStr = response.debugString();
        EXPECT_FALSE(debugStr.empty());
    });
}

// ============================================================================
// Backward Compatibility Tests
// ============================================================================

class BackwardCompatibilityTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(BackwardCompatibilityTest, ExistingServiceReplyStillWorks) {
    // Test that existing code using ServiceReply with proto messages still works
    auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
    
    // This test validates that our changes don't break existing functionality
    ServiceReply reply1; // Default constructor
    EXPECT_FALSE(reply1.hasMarketDataStreamResponse());
    
    // Test that ptr() still works as expected
    EXPECT_EQ(reply1.ptr(), nullptr);
}

TEST_F(BackwardCompatibilityTest, ServiceReplyPrepareServiceAnswerStillWorks) {
    // Test template method still works
    GetTradingStatusResponse protoResponse;
    protoResponse.set_trading_status(SecurityTradingStatus::SECURITY_TRADING_STATUS_NORMAL_TRADING);
    
    Status status;
    auto reply = ServiceReply::prepareServiceAnswer(status, protoResponse);
    
    EXPECT_NE(reply.ptr(), nullptr);
    EXPECT_FALSE(reply.hasMarketDataStreamResponse()); // Should still be false for non-stream responses
}

// ============================================================================
// Copy and Move Semantics Tests
// ============================================================================

class MarketDataStreamResponseCopyMoveTest : public ::testing::Test {
protected:
    MarketDataResponse createTradingStatusResponse() {
        MarketDataResponse response;
        response.mutable_trading_status()->set_figi("BBG000B9XRY4");
        return response;
    }
};

TEST_F(MarketDataStreamResponseCopyMoveTest, CopyConstructor) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse original(rawResponse);
    
    MarketDataStreamResponse copy(original);
    
    EXPECT_EQ(copy.getPayloadType(), original.getPayloadType());
    EXPECT_EQ(copy.getTradingStatus().figi(), original.getTradingStatus().figi());
}

TEST_F(MarketDataStreamResponseCopyMoveTest, CopyAssignment) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse original(rawResponse);
    
    MarketDataStreamResponse copy;
    copy = original;
    
    EXPECT_EQ(copy.getPayloadType(), original.getPayloadType());
    EXPECT_EQ(copy.getTradingStatus().figi(), original.getTradingStatus().figi());
}

TEST_F(MarketDataStreamResponseCopyMoveTest, MoveConstructor) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse original(rawResponse);
    auto originalType = original.getPayloadType();
    
    MarketDataStreamResponse moved(std::move(original));
    
    EXPECT_EQ(moved.getPayloadType(), originalType);
}

TEST_F(MarketDataStreamResponseCopyMoveTest, MoveAssignment) {
    auto rawResponse = createTradingStatusResponse();
    MarketDataStreamResponse original(rawResponse);
    auto originalType = original.getPayloadType();
    
    MarketDataStreamResponse moved;
    moved = std::move(original);
    
    EXPECT_EQ(moved.getPayloadType(), originalType);
}

// ============================================================================
// Integration Test with Real gRPC (If Available)
// ============================================================================

#ifdef INTEGRATION_TEST_ENABLED

class MarketDataStreamIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(
            "invest-public-api.tinkoff.ru:443", 
            grpc::SslCredentials(grpc::SslTLSRootCerts())
        );
        // Would need real token for integration tests
    }
};

// These would be real integration tests when running against actual API
TEST_F(MarketDataStreamIntegrationTest, RealTradingStatusStream) {
    GTEST_SKIP() << "Requires real API token and connection";
    // Test real trading status streaming
}

#endif

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
