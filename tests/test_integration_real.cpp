/**
 * @file test_integration_real.cpp
 * @brief Real API Integration Tests for TinkoffInvestSDK
 * 
 * WARNING: These tests make actual API calls to Tinkoff Invest API.
 * Set the TINKOFF_TOKEN environment variable before running.
 * 
 * These tests require:
 * 1. A valid Tinkoff Invest API token
 * 2. Access to the sandbox environment (recommended for testing)
 * 3. Network connectivity to invest-public-api.tinkoff.ru:443
 * 
 * Usage:
 *   export TINKOFF_TOKEN="your_token_here"
 *   ./tests/TinkoffInvestSDKTests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <cstdlib>
#include <memory>
#include <string>
#include <chrono>

#include "investapiclient.h"
#include "sandboxservice.h"
#include "marketdataservice.h"
#include "usersservice.h"
#include "instrumentsservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// Configuration
const std::string SANDBOX_HOST = "invest-public-api.tinkoff.ru:443";
const std::string REAL_HOST = "invest-public-api.tinkoff.ru:443";

// Helper function to get token from environment
std::string getToken() {
    const char* token = std::getenv("TINKOFF_TOKEN");
    if (token == nullptr || std::string(token).empty()) {
        return "test_token_placeholder";
    }
    return std::string(token);
}

bool isTokenValid() {
    std::string token = getToken();
    return !token.empty() && token != "test_token_placeholder";
}

// Helper function to get first account ID from API
std::string getFirstAccountId(Users* users) {
    if (users == nullptr) {
        return "";
    }
    auto reply = users->GetAccounts();
    if (reply.GetStatus().ok() && reply.accountCount() > 0) {
        return reply.accountID(0);
    }
    return "";
}

// ============================================================================
// Real Sandbox Integration Tests
// ============================================================================

class SandboxIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!isTokenValid()) {
            GTEST_SKIP() << "TINKOFF_TOKEN environment variable not set. Skipping integration tests.";
        }
        token = getToken();
        auto channel = grpc::CreateChannel(SANDBOX_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        sandbox = std::make_shared<Sandbox>(channel, token);
    }

    void TearDown() override {
        // Clean up: close any accounts we opened
        if (isTokenValid() && sandbox) {
            auto accountsReply = sandbox->GetSandboxAccounts();
            if (accountsReply.GetStatus().ok()) {
                for (int i = 0; i < accountsReply.accountCount(); ++i) {
                    sandbox->CloseSandboxAccount(accountsReply.accountID(i));
                }
            }
        }
    }

    std::string token;
    std::shared_ptr<Sandbox> sandbox;
};

TEST_F(SandboxIntegrationTest, OpenSandboxAccountSuccess) {
    ASSERT_NE(sandbox, nullptr);
    
    auto reply = sandbox->OpenSandboxAccount();
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to open sandbox account: " 
        << reply.GetStatus().error_message();
    ASSERT_NE(reply.ptr(), nullptr);
    
    std::cout << "Opened sandbox account successfully" << std::endl;
}

TEST_F(SandboxIntegrationTest, GetSandboxAccountsAfterOpening) {
    // First open an account
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    // Then get accounts
    auto accountsReply = sandbox->GetSandboxAccounts();
    
    ASSERT_TRUE(accountsReply.GetStatus().ok()) << "Failed to get accounts: "
        << accountsReply.GetStatus().error_message();
    ASSERT_GE(accountsReply.accountCount(), 1) << "No accounts found";
    
    // Verify account has valid ID
    std::string accountId = accountsReply.accountID(0);
    EXPECT_FALSE(accountId.empty());
    
    std::cout << "Found " << accountsReply.accountCount() << " account(s)" << std::endl;
}

TEST_F(SandboxIntegrationTest, CloseSandboxAccountSuccess) {
    // Open an account first
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    std::string accountId = sandbox->GetSandboxAccounts().accountID(0);
    ASSERT_FALSE(accountId.empty());
    
    // Close the account
    auto closeReply = sandbox->CloseSandboxAccount(accountId);
    
    ASSERT_TRUE(closeReply.GetStatus().ok()) << "Failed to close account: "
        << closeReply.GetStatus().error_message();
    
    std::cout << "Closed sandbox account: " << accountId << std::endl;
}

TEST_F(SandboxIntegrationTest, SandboxPayInSuccess) {
    // Open an account first
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    std::string accountId = sandbox->GetSandboxAccounts().accountID(0);
    ASSERT_FALSE(accountId.empty());
    
    // Deposit funds
    auto payInReply = sandbox->SandboxPayIn(accountId, "USD", 10000, 0);
    
    ASSERT_TRUE(payInReply.GetStatus().ok()) << "Failed to pay in: "
        << payInReply.GetStatus().error_message();
    
    std::cout << "Deposited funds to account: " << accountId << std::endl;
}

TEST_F(SandboxIntegrationTest, GetSandboxPortfolioAfterPayIn) {
    // Open account and deposit funds
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    std::string accountId = sandbox->GetSandboxAccounts().accountID(0);
    ASSERT_FALSE(accountId.empty());
    
    sandbox->SandboxPayIn(accountId, "RUB", 100000, 0);
    
    // Get portfolio
    auto portfolioReply = sandbox->GetSandboxPortfolio(accountId, PortfolioRequest_CurrencyRequest_RUB);
    
    ASSERT_TRUE(portfolioReply.GetStatus().ok()) << "Failed to get portfolio: "
        << portfolioReply.GetStatus().error_message();
    ASSERT_NE(portfolioReply.ptr(), nullptr);
    
    std::cout << "Retrieved portfolio for account: " << accountId << std::endl;
}

TEST_F(SandboxIntegrationTest, PostAndGetSandboxOrder) {
    // Open account and deposit funds
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    std::string accountId = sandbox->GetSandboxAccounts().accountID(0);
    ASSERT_FALSE(accountId.empty());
    
    sandbox->SandboxPayIn(accountId, "USD", 10000, 0);
    
    // Post an order (using a real instrument ID for a US stock)
    auto orderReply = sandbox->PostSandboxOrder(
        "BBG000B9XRY4",  // Apple Inc. FIGI
        10,              // quantity
        15000,           // price units ($150.00)
        0,               // price nano
        OrderDirection::ORDER_DIRECTION_BUY,
        accountId,
        OrderType::ORDER_TYPE_LIMIT,
        "test_order_001"
    );
    
    ASSERT_TRUE(orderReply.GetStatus().ok()) << "Failed to post order: "
        << orderReply.GetStatus().error_message();
    
    // Get orders
    auto ordersReply = sandbox->GetSandboxOrders(accountId);
    ASSERT_TRUE(ordersReply.GetStatus().ok());
    
    std::cout << "Posted and retrieved order successfully" << std::endl;
}

// ============================================================================
// Real MarketData Integration Tests
// ============================================================================

class MarketDataIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!isTokenValid()) {
            GTEST_SKIP() << "TINKOFF_TOKEN environment variable not set. Skipping integration tests.";
        }
        token = getToken();
        auto channel = grpc::CreateChannel(SANDBOX_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        marketdata = std::make_shared<MarketData>(channel, token);
    }

    std::string token;
    std::shared_ptr<MarketData> marketdata;
};

TEST_F(MarketDataIntegrationTest, GetLastPricesSuccess) {
    ASSERT_NE(marketdata, nullptr);
    
    // Get prices for common US stocks
    std::vector<std::string> instruments = {
        "BBG000B9XRY4",  // Apple
        "BBG000BJP3S4",  // Microsoft
        "BBG000BM8D53"   // Amazon
    };
    
    auto reply = marketdata->GetLastPrices(instruments);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get last prices: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetLastPricesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->last_prices_size() << " last prices" << std::endl;
}

TEST_F(MarketDataIntegrationTest, GetOrderBookSuccess) {
    ASSERT_NE(marketdata, nullptr);
    
    auto reply = marketdata->GetOrderBook("BBG000B9XRY4", 10);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get order book: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetOrderBookResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->depth(), 10);
    
    std::cout << "Order book depth: " << response->bids_size() << " bids, "
              << response->asks_size() << " asks" << std::endl;
}

TEST_F(MarketDataIntegrationTest, GetTradingStatusSuccess) {
    ASSERT_NE(marketdata, nullptr);
    
    auto reply = marketdata->GetTradingStatus("BBG000B9XRY4");
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get trading status: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetTradingStatusResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Trading status for " << response->figi() 
        << ": market_order_available=" << response->market_order_available_flag() << std::endl;
}

TEST_F(MarketDataIntegrationTest, GetCandlesSuccess) {
    ASSERT_NE(marketdata, nullptr);
    
    auto now = std::chrono::system_clock::now();
    auto yesterday = now - std::chrono::hours(24);
    
    auto now_ts = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    auto yesterday_ts = std::chrono::duration_cast<std::chrono::seconds>(yesterday.time_since_epoch()).count();
    
    auto reply = marketdata->GetCandles(
        "BBG000B9XRY4",
        yesterday_ts, 0,
        now_ts, 0,
        CandleInterval::CANDLE_INTERVAL_HOUR
    );
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get candles: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetCandlesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->candles_size() << " candles" << std::endl;
}

// ============================================================================
// Real Users Integration Tests
// ============================================================================

class UsersIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!isTokenValid()) {
            GTEST_SKIP() << "TINKOFF_TOKEN environment variable not set. Skipping integration tests.";
        }
        token = getToken();
        auto channel = grpc::CreateChannel(SANDBOX_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        users = std::make_shared<Users>(channel, token);
    }

    std::string token;
    std::shared_ptr<Users> users;
};

TEST_F(UsersIntegrationTest, GetAccountsSuccess) {
    ASSERT_NE(users, nullptr);
    
    auto reply = users->GetAccounts();
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get accounts: "
        << reply.GetStatus().error_message();
    
    std::cout << "Retrieved " << reply.accountCount() << " account(s)" << std::endl;
}

TEST_F(UsersIntegrationTest, GetInfoSuccess) {
    ASSERT_NE(users, nullptr);
    
    auto reply = users->GetInfo();
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get user info: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetInfoResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "User info retrieved successfully" << std::endl;
}

TEST_F(UsersIntegrationTest, GetMarginAttributesSuccess) {
    ASSERT_NE(users, nullptr);
    
    // First get accounts
    auto accountsReply = users->GetAccounts();
    if (accountsReply.accountCount() == 0) {
        GTEST_SKIP() << "No accounts available for margin test";
    }
    
    std::string accountId = accountsReply.accountID(0);
    
    auto reply = users->GetMarginAttributes(accountId);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get margin attributes: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<GetMarginAttributesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Margin attributes retrieved successfully" << std::endl;
}

// ============================================================================
// Real Instruments Integration Tests
// ============================================================================

class InstrumentsIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!isTokenValid()) {
            GTEST_SKIP() << "TINKOFF_TOKEN environment variable not set. Skipping integration tests.";
        }
        token = getToken();
        auto channel = grpc::CreateChannel(SANDBOX_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        instruments = std::make_shared<Instruments>(channel, token);
    }

    std::string token;
    std::shared_ptr<Instruments> instruments;
};

TEST_F(InstrumentsIntegrationTest, GetSharesSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->Shares(INSTRUMENT_STATUS_ALL);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get shares: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<SharesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->instruments_size() << " shares" << std::endl;
}

TEST_F(InstrumentsIntegrationTest, GetBondsSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->Bonds(INSTRUMENT_STATUS_ALL);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get bonds: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<BondsResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->instruments_size() << " bonds" << std::endl;
}

TEST_F(InstrumentsIntegrationTest, GetEtfsSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->Etfs(INSTRUMENT_STATUS_ALL);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get ETFs: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<EtfsResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->instruments_size() << " ETFs" << std::endl;
}

TEST_F(InstrumentsIntegrationTest, GetCurrenciesSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->Currencies(INSTRUMENT_STATUS_ALL);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get currencies: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<CurrenciesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->instruments_size() << " currencies" << std::endl;
}

TEST_F(InstrumentsIntegrationTest, GetFuturesSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->Futures(INSTRUMENT_STATUS_ALL);
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get futures: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<FuturesResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Retrieved " << response->instruments_size() << " futures" << std::endl;
}

TEST_F(InstrumentsIntegrationTest, ShareByFigiSuccess) {
    ASSERT_NE(instruments, nullptr);
    
    auto reply = instruments->ShareBy(
        INSTRUMENT_ID_TYPE_FIGI,
        "",
        "BBG000B9XRY4"  // Apple
    );
    
    ASSERT_TRUE(reply.GetStatus().ok()) << "Failed to get share by FIGI: "
        << reply.GetStatus().error_message();
    
    auto response = std::static_pointer_cast<ShareResponse>(reply.ptr());
    ASSERT_NE(response, nullptr);
    
    std::cout << "Share lookup by FIGI completed successfully" << std::endl;
}

// ============================================================================
// Full Workflow Integration Test
// ============================================================================

class FullWorkflowIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!isTokenValid()) {
            GTEST_SKIP() << "TINKOFF_TOKEN environment variable not set. Skipping integration tests.";
        }
        token = getToken();
        auto channel = grpc::CreateChannel(SANDBOX_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        
        sandbox = std::make_shared<Sandbox>(channel, token);
        marketdata = std::make_shared<MarketData>(channel, token);
        users = std::make_shared<Users>(channel, token);
    }

    std::string token;
    std::shared_ptr<Sandbox> sandbox;
    std::shared_ptr<MarketData> marketdata;
    std::shared_ptr<Users> users;
};

TEST_F(FullWorkflowIntegrationTest, CompleteTradingWorkflow) {
    // Step 1: Get user info
    auto infoReply = users->GetInfo();
    ASSERT_TRUE(infoReply.GetStatus().ok());
    
    // Step 2: Open sandbox account
    auto openReply = sandbox->OpenSandboxAccount();
    ASSERT_TRUE(openReply.GetStatus().ok());
    
    std::string accountId = sandbox->GetSandboxAccounts().accountID(0);
    ASSERT_FALSE(accountId.empty());
    
    // Step 3: Get current market data
    auto pricesReply = marketdata->GetLastPrices({"BBG000B9XRY4"});
    ASSERT_TRUE(pricesReply.GetStatus().ok());
    
    // Step 4: Deposit funds
    auto payInReply = sandbox->SandboxPayIn(accountId, "USD", 10000, 0);
    ASSERT_TRUE(payInReply.GetStatus().ok());
    
    // Step 5: Get portfolio
    auto portfolioReply = sandbox->GetSandboxPortfolio(accountId, PortfolioRequest_CurrencyRequest_RUB);
    ASSERT_TRUE(portfolioReply.GetStatus().ok());
    
    // Step 6: Close account
    auto closeReply = sandbox->CloseSandboxAccount(accountId);
    ASSERT_TRUE(closeReply.GetStatus().ok());
    
    std::cout << "Complete trading workflow executed successfully!" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Real API Integration Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    
    if (!isTokenValid()) {
        std::cout << "\nWARNING: TINKOFF_TOKEN environment variable not set!" << std::endl;
        std::cout << "Integration tests will be SKIPPED." << std::endl;
        std::cout << "To run integration tests:" << std::endl;
        std::cout << "  export TINKOFF_TOKEN=\"your_token_here\"" << std::endl;
        std::cout << "  ./tests/TinkoffInvestSDKTests" << std::endl;
    } else {
        std::cout << "\nToken found. Running integration tests..." << std::endl;
    }
    
    std::cout << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

