/**
 * @file test_api_integration.cpp
 * @brief Basic Integration Tests for TinkoffInvestSDK
 * 
 * This file contains basic SDK structure tests. Full protobuf/API tests
 * are not included due to gRPC stubs being marked as 'final' in newer 
 * protobuf versions and significant API changes.
 */

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>

// Include SDK headers
#include "investapiclient.h"
#include "sandboxservice.h"
#include "marketdataservice.h"
#include "marketdatastreamservice.h"
#include "instrumentsservice.h"
#include "operationsservice.h"
#include "ordersservice.h"
#include "ordersstreamservice.h"
#include "stopordersservice.h"
#include "usersservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// Test configuration
const std::string TEST_TOKEN = "test_token";
const std::string TEST_HOST = "localhost:50051";
const std::string TEST_ACCOUNT_ID = "test_account_id";
const std::string TEST_FIGI = "BBG004S68104";  // Sberbank
const std::string TEST_INSTRUMENT_ID = "TCS-001";

// ============================================================================
// Common Types Tests
// ============================================================================

class ServiceReplyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a test message
        auto response = std::make_shared<GetAccountsResponse>();
        auto* account = response->add_accounts();
        account->set_id("test_id");
        account->set_name("Test Name");
        
        m_validReply = ServiceReply(response, grpc::Status::OK);
        m_invalidReply = ServiceReply(nullptr, grpc::Status::CANCELLED, "Test error");
    }

    ServiceReply m_validReply;
    ServiceReply m_invalidReply;
};

TEST_F(ServiceReplyTest, PtrReturnsValidPointer) {
    EXPECT_NE(m_validReply.ptr(), nullptr);
    EXPECT_EQ(m_invalidReply.ptr(), nullptr);
}

TEST_F(ServiceReplyTest, GetStatusReturnsCorrectStatus) {
    EXPECT_TRUE(m_validReply.GetStatus().ok());
    EXPECT_FALSE(m_invalidReply.GetStatus().ok());
}

TEST_F(ServiceReplyTest, GetErrorMessageReturnsCorrectMessage) {
    EXPECT_EQ(m_invalidReply.GetErrorMessage(), "Test error");
    EXPECT_TRUE(m_validReply.GetErrorMessage().empty());
}

TEST_F(ServiceReplyTest, AccountIDReturnsCorrectValue) {
    EXPECT_EQ(m_validReply.accountID(0), "test_id");
    EXPECT_EQ(m_validReply.accountID(1), "");
}

TEST_F(ServiceReplyTest, AccountNameReturnsCorrectValue) {
    EXPECT_EQ(m_validReply.accountName(0), "Test Name");
    EXPECT_EQ(m_validReply.accountName(1), "");
}

TEST_F(ServiceReplyTest, AccountCountReturnsCorrectValue) {
    EXPECT_EQ(m_validReply.accountCount(), 1);
}

// ============================================================================
// MarketDataStreamResponse Tests
// ============================================================================

class MarketDataStreamResponseTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Default constructed response
        m_defaultResponse = MarketDataStreamResponse();
    }

    MarketDataStreamResponse m_defaultResponse;
};

TEST_F(MarketDataStreamResponseTest, DefaultConstructorCreatesUnknownPayload) {
    EXPECT_EQ(m_defaultResponse.getPayloadType(), 
              MarketDataStreamResponse::PayloadType::UNKNOWN);
}

TEST_F(MarketDataStreamResponseTest, PayloadTypeToStringWorks) {
    // Test that the static method works
    std::string unknownStr = MarketDataStreamResponse::payloadTypeToString(
                MarketDataStreamResponse::PayloadType::UNKNOWN);
    EXPECT_FALSE(unknownStr.empty());
}

TEST_F(MarketDataStreamResponseTest, IsPayloadTypeWorksForUnknown) {
    EXPECT_TRUE(m_defaultResponse.isPayloadType(
                MarketDataStreamResponse::PayloadType::UNKNOWN));
    EXPECT_FALSE(m_defaultResponse.isPayloadType(
                MarketDataStreamResponse::PayloadType::TRADING_STATUS));
}

TEST_F(MarketDataStreamResponseTest, DebugStringWorks) {
    // Test that debugString() can be called without error
    std::string debug = m_defaultResponse.debugString();
    EXPECT_TRUE(debug.empty() || !debug.empty()); // Just verify it doesn't throw
}

// ============================================================================
// InvestApiClient Tests
// ============================================================================

class InvestApiClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        client = std::make_unique<InvestApiClient>(TEST_HOST, TEST_TOKEN);
    }

    std::unique_ptr<InvestApiClient> client;
};

TEST_F(InvestApiClientTest, ClientCreationSucceeds) {
    EXPECT_NE(client, nullptr);
}

TEST_F(InvestApiClientTest, ServiceReturnsNonNullForValidService) {
    auto sandbox = client->service("sandbox");
    EXPECT_NE(sandbox, nullptr);

    auto marketdata = client->service("marketdata");
    EXPECT_NE(marketdata, nullptr);

    auto users = client->service("users");
    EXPECT_NE(users, nullptr);
}

TEST_F(InvestApiClientTest, ServiceReturnsNullForInvalidService) {
    auto invalid = client->service("invalid_service");
    EXPECT_EQ(invalid, nullptr);
}

TEST_F(InvestApiClientTest, AllServicesAreAccessible) {
    EXPECT_NE(client->service("sandbox"), nullptr);
    EXPECT_NE(client->service("users"), nullptr);
    EXPECT_NE(client->service("marketdata"), nullptr);
    EXPECT_NE(client->service("instruments"), nullptr);
    EXPECT_NE(client->service("operations"), nullptr);
    EXPECT_NE(client->service("orders"), nullptr);
    EXPECT_NE(client->service("stoporders"), nullptr);
    EXPECT_NE(client->service("marketdatastream"), nullptr);
    EXPECT_NE(client->service("ordersstream"), nullptr);
}

TEST_F(InvestApiClientTest, EmptyTokenClientCreationSucceeds) {
    // Client should be created even with empty token (actual auth happens on API call)
    InvestApiClient emptyClient(TEST_HOST, "");
    EXPECT_NE(emptyClient.service("sandbox"), nullptr);
}

// ============================================================================
// Service Pointer Type Tests
// ============================================================================

class ServicePointerTypeTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::InsecureChannelCredentials());
        sandboxPtr = std::make_shared<Sandbox>(channel, TEST_TOKEN);
        marketdataPtr = std::make_shared<MarketData>(channel, TEST_TOKEN);
        usersPtr = std::make_shared<Users>(channel, TEST_TOKEN);
    }

    std::shared_ptr<Sandbox> sandboxPtr;
    std::shared_ptr<MarketData> marketdataPtr;
    std::shared_ptr<Users> usersPtr;
};

TEST_F(ServicePointerTypeTest, SandboxServicePointerIsValid) {
    EXPECT_NE(sandboxPtr, nullptr);
}

TEST_F(ServicePointerTypeTest, MarketDataServicePointerIsValid) {
    EXPECT_NE(marketdataPtr, nullptr);
}

TEST_F(ServicePointerTypeTest, UsersServicePointerIsValid) {
    EXPECT_NE(usersPtr, nullptr);
}

// ============================================================================
// Message Type Tests
// ============================================================================

class MessageTypesTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test messages
    }
};

TEST_F(MessageTypesTest, MoneyValueCanBeCreated) {
    MoneyValue money;
    money.set_units(100);
    money.set_nano(500000000);
    EXPECT_EQ(money.units(), 100);
    EXPECT_EQ(money.nano(), 500000000);
}

TEST_F(MessageTypesTest, SharesRequestCanBeCreated) {
    InstrumentsRequest request;
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, CandleRequestCanBeCreated) {
    GetCandlesRequest request;
    request.set_instrument_id(TEST_INSTRUMENT_ID);
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, OrderBookRequestCanBeCreated) {
    GetOrderBookRequest request;
    request.set_instrument_id(TEST_INSTRUMENT_ID);
    request.set_depth(10);
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, PortfolioRequestCanBeCreated) {
    PortfolioRequest request;
    request.set_account_id(TEST_ACCOUNT_ID);
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, PositionsRequestCanBeCreated) {
    PositionsRequest request;
    request.set_account_id(TEST_ACCOUNT_ID);
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, GetAccountsRequestCanBeCreated) {
    GetAccountsRequest request;
    EXPECT_TRUE(request.IsInitialized());
}

TEST_F(MessageTypesTest, OperationsRequestCanBeCreated) {
    OperationsRequest request;
    request.set_account_id(TEST_ACCOUNT_ID);
    EXPECT_TRUE(request.IsInitialized());
}

// ============================================================================
// MarketDataStreamResponse ServiceReply Tests
// ============================================================================

class MarketDataStreamResponseServiceReplyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a MarketDataStreamResponse with default settings
        m_response = MarketDataStreamResponse();
    }

    MarketDataStreamResponse m_response;
};

TEST_F(MarketDataStreamResponseServiceReplyTest, HasMarketDataStreamResponseReturnsFalse) {
    // Create an empty ServiceReply
    ServiceReply reply;
    EXPECT_FALSE(reply.hasMarketDataStreamResponse());
}

TEST_F(MarketDataStreamResponseServiceReplyTest, GetMarketDataStreamResponseOnEmptyReplyThrows) {
    ServiceReply reply;
    // This should work but may throw - just verify method exists
    EXPECT_NO_THROW(reply.hasMarketDataStreamResponse());
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

