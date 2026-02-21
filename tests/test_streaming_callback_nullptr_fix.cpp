/**
 * @file test_streaming_callback nullptr.cpp
 * @brief Test for verifying that streaming callbacks return valid data
 * 
 * This test specifically verifies the fix for the issue where market data stream
 * callbacks were returning nullptr because the code was using ptr() instead of
 * getMarketDataStreamResponse() for streaming responses.
 * 
 * The issue: When using streaming methods like SubscribeLastPriceAsync, the callback
 * receives a ServiceReply where ptr() returns nullptr because the data is stored in
 * m_streamResponse, not m_replyPtr.
 * 
 * The fix: Use hasMarketDataStreamResponse() and getMarketDataStreamResponse() 
 * to properly access streaming response data.
 * 
 * Note: This test verifies the fix by checking callback handling logic.
 * It tests whether ServiceReply correctly handles streaming responses.
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

// Include SDK headers
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "marketdatastreamresponse.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// ============================================================================
// Unit Test: Verify ServiceReply handles streaming responses correctly
// ============================================================================

class ServiceReplyStreamingTest : public ::testing::Test {
protected:
    // No SetUp needed for unit tests
};

// Test that ServiceReply correctly identifies streaming responses
TEST_F(ServiceReplyStreamingTest, StreamingResponseHasCorrectData)
{
    std::cout << "[Test] Testing ServiceReply handling of streaming responses..." << std::endl;
    
    // Create a MarketDataResponse with some data - just set figi which is available
    MarketDataResponse response;
    response.mutable_last_price()->set_figi("BBG004S68104");
    
    // Create MarketDataStreamResponse from the raw response
    MarketDataStreamResponse streamResponse(response);
    
    // Verify the payload type detection works
    EXPECT_EQ(streamResponse.getPayloadType(), MarketDataStreamResponse::PayloadType::LAST_PRICE);
    std::cout << "[Test] Payload type: " << streamResponse.getPayloadTypeString() << std::endl;
    
    // Create ServiceReply with the streaming response
    ServiceReply reply(streamResponse);
    
    // THE FIX: Verify that hasMarketDataStreamResponse() returns true
    EXPECT_TRUE(reply.hasMarketDataStreamResponse()) 
        << "hasMarketDataStreamResponse() should return true for streaming responses";
    
    // Verify that ptr() returns nullptr (this is the root cause of the bug)
    EXPECT_EQ(reply.ptr(), nullptr) 
        << "ptr() should return nullptr for streaming responses";
    
    // THE FIX: Verify that getMarketDataStreamResponse() returns valid data
    const auto& retrievedResponse = reply.getMarketDataStreamResponse();
    EXPECT_EQ(retrievedResponse.getPayloadType(), MarketDataStreamResponse::PayloadType::LAST_PRICE);
    
    std::cout << "[Test] Successfully retrieved streaming response via getMarketDataStreamResponse()" << std::endl;
    std::cout << "[Test] Test passed!" << std::endl;
}

// Test with Candle payload type (streaming Candle only has figi)
TEST_F(ServiceReplyStreamingTest, CandleStreamingResponse)
{
    MarketDataResponse response;
    response.mutable_candle()->set_figi("BBG004S68104");
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.ptr(), nullptr);  // ptr() returns nullptr for streaming
    
    const auto& retrieved = reply.getMarketDataStreamResponse();
    EXPECT_EQ(retrieved.getPayloadType(), MarketDataStreamResponse::PayloadType::CANDLE);
    
    std::cout << "[Test] Candle streaming response test passed!" << std::endl;
}

// Test with OrderBook payload type
TEST_F(ServiceReplyStreamingTest, OrderBookStreamingResponse)
{
    MarketDataResponse response;
    response.mutable_orderbook()->set_figi("BBG004S68104");
    response.mutable_orderbook()->set_depth(10);
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.ptr(), nullptr);
    
    const auto& retrieved = reply.getMarketDataStreamResponse();
    EXPECT_EQ(retrieved.getPayloadType(), MarketDataStreamResponse::PayloadType::ORDERBOOK);
    
    std::cout << "[Test] OrderBook streaming response test passed!" << std::endl;
}

// Test with Trade payload type
TEST_F(ServiceReplyStreamingTest, TradeStreamingResponse)
{
    MarketDataResponse response;
    response.mutable_trade()->set_figi("BBG004S68104");
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.ptr(), nullptr);
    
    const auto& retrieved = reply.getMarketDataStreamResponse();
    EXPECT_EQ(retrieved.getPayloadType(), MarketDataStreamResponse::PayloadType::TRADE);
    
    std::cout << "[Test] Trade streaming response test passed!" << std::endl;
}

// Test with TradingStatus payload type
TEST_F(ServiceReplyStreamingTest, TradingStatusStreamingResponse)
{
    MarketDataResponse response;
    response.mutable_trading_status()->set_figi("BBG004S68104");
    response.mutable_trading_status()->set_trading_status(SecurityTradingStatus::SECURITY_TRADING_STATUS_NORMAL_TRADING);
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.ptr(), nullptr);
    
    const auto& retrieved = reply.getMarketDataStreamResponse();
    EXPECT_EQ(retrieved.getPayloadType(), MarketDataStreamResponse::PayloadType::TRADING_STATUS);
    
    std::cout << "[Test] TradingStatus streaming response test passed!" << std::endl;
}

// Test with subscription response payload type
TEST_F(ServiceReplyStreamingTest, SubscriptionResponse)
{
    MarketDataResponse response;
    response.mutable_subscribe_last_price_response()->set_tracking_id("test-tracking-id");
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    EXPECT_TRUE(reply.hasMarketDataStreamResponse());
    EXPECT_EQ(reply.ptr(), nullptr);
    
    const auto& retrieved = reply.getMarketDataStreamResponse();
    EXPECT_TRUE(retrieved.isSubscriptionResponse());
    
    std::cout << "[Test] Subscription response test passed!" << std::endl;
}

// Test the callback pattern that should be used (matching the fix)
TEST_F(ServiceReplyStreamingTest, CallbackPatternTest)
{
    std::cout << "[Test] Testing the correct callback pattern..." << std::endl;
    
    // Simulate what the fixed callback should do
    auto testCallback = [](ServiceReply reply) {
        // THE FIX: Check for streaming response first
        if (reply.hasMarketDataStreamResponse()) {
            const auto& streamResponse = reply.getMarketDataStreamResponse();
            std::string payloadType = streamResponse.getPayloadTypeString();
            std::cout << "[Callback] Streaming response type: " << payloadType << std::endl;
            
            // Should be able to get debug string
            std::string debugStr = streamResponse.debugString();
            EXPECT_FALSE(debugStr.empty());
            return;
        }
        
        // Fallback for non-streaming responses
        if (reply.ptr()) {
            std::cout << "[Callback] Non-streaming response" << std::endl;
            return;
        }
        
        std::cout << "[Callback] Empty response" << std::endl;
    };
    
    // Test with streaming response
    MarketDataResponse response;
    response.mutable_last_price()->set_figi("BBG004S68104");
    
    MarketDataStreamResponse streamResponse(response);
    ServiceReply reply(streamResponse);
    
    // This should not crash (the original bug would cause crash here with ptr()->DebugString())
    EXPECT_NO_FATAL_FAILURE(testCallback(reply));
    
    std::cout << "[Test] Callback pattern test passed!" << std::endl;
}

// Test: Verify the bug - ptr() returns nullptr for streaming
TEST_F(ServiceReplyStreamingTest, VerifyPtrReturnsNullptrForStreaming)
{
    std::cout << "[Test] Verifying that ptr() returns nullptr for streaming responses..." << std::endl;
    
    // Create various streaming response types - only use available fields
    std::vector<MarketDataResponse> responses;
    
    MarketDataResponse r1;
    r1.mutable_last_price()->set_figi("BBG004S68104");
    responses.push_back(r1);
    
    MarketDataResponse r2;
    r2.mutable_candle()->set_figi("BBG004S68104");
    responses.push_back(r2);
    
    MarketDataResponse r3;
    r3.mutable_orderbook()->set_figi("BBG004S68104");
    responses.push_back(r3);
    
    MarketDataResponse r4;
    r4.mutable_trade()->set_figi("BBG004S68104");
    responses.push_back(r4);
    
    MarketDataResponse r5;
    r5.mutable_trading_status()->set_figi("BBG004S68104");
    responses.push_back(r5);
    
    MarketDataResponse r6;
    r6.mutable_subscribe_last_price_response()->set_tracking_id("test");
    responses.push_back(r6);
    
    for (size_t i = 0; i < responses.size(); ++i) {
        MarketDataStreamResponse streamResp(responses[i]);
        ServiceReply reply(streamResp);
        
        // Verify hasMarketDataStreamResponse returns true
        EXPECT_TRUE(reply.hasMarketDataStreamResponse()) 
            << "Response " << i << ": hasMarketDataStreamResponse should return true";
        
        // Verify ptr() returns nullptr
        EXPECT_EQ(reply.ptr(), nullptr) 
            << "Response " << i << ": ptr() should return nullptr for streaming responses";
        
        // Verify getMarketDataStreamResponse() works
        EXPECT_NO_FATAL_FAILURE(reply.getMarketDataStreamResponse());
    }
    
    std::cout << "[Test] Verified ptr() returns nullptr for all streaming response types!" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

