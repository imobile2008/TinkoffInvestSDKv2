/**
 * @file simple_payload_test.cpp
 * @brief Simple standalone test for MarketDataStreamResponse payload type detection
 */

#include <iostream>
#include <cassert>
#include "services/marketdatastreamresponse.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// Mock MarketDataResponse for testing (simplified version)
class MockMarketDataResponse {
public:
    bool has_trading_status_ = false;
    bool has_subscribe_info_response_ = false;
    bool has_candle_ = false;
    
    MockMarketDataResponse& mutable_trading_status() { 
        has_trading_status_ = true; 
        return *this; 
    }
    
    MockMarketDataResponse& mutable_subscribe_info_response() { 
        has_subscribe_info_response_ = true; 
        return *this; 
    }
    
    MockMarketDataResponse& mutable_candle() { 
        has_candle_ = true; 
        return *this; 
    }
};

// Test payload type detection
void testPayloadTypeDetection() {
    std::cout << "Testing payload type detection..." << std::endl;
    
    // Test UNKNOWN
    MarketDataStreamResponse response1;
    assert(response1.getPayloadType() == MarketDataStreamResponse::PayloadType::UNKNOWN);
    std::cout << "✓ UNKNOWN payload type detection works" << std::endl;
    
    // Test payload type to string conversion
    assert(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::TRADING_STATUS) == "TRADING_STATUS");
    assert(MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE) == "SUBSCRIBE_INFO_RESPONSE");
    std::cout << "✓ Payload type string conversion works" << std::endl;
}

void testServiceReplyIntegration() {
    std::cout << "\nTesting ServiceReply integration..." << std::endl;
    
    ServiceReply reply1;
    assert(!reply1.hasMarketDataStreamResponse());
    std::cout << "✓ Default ServiceReply has no stream response" << std::endl;
    
    MarketDataStreamResponse streamResponse;
    ServiceReply reply2(streamResponse);
    assert(reply2.hasMarketDataStreamResponse());
    std::cout << "✓ ServiceReply with stream response works" << std::endl;
    
    // Verify we can access the stream response
    const auto& retrievedResponse = reply2.getMarketDataStreamResponse();
    assert(retrievedResponse.getPayloadType() == MarketDataStreamResponse::PayloadType::UNKNOWN);
    std::cout << "✓ ServiceReply stream response access works" << std::endl;
}

int main() {
    std::cout << "=== MarketDataStreamResponse Test Suite ===" << std::endl;
    
    try {
        testPayloadTypeDetection();
        testServiceReplyIntegration();
        
        std::cout << "\n✅ All tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
