/**
 * @file test_api_tokens.cpp
 * @brief Sandbox API tests using provided sandbox token
 * 
 * Usage:
 *   export TINKOFF_TOKEN="your_sandbox_token_here"
 *   ./tests/test_api_tokens
 * 
 * Note: Sandbox tokens can only be used with sandbox service at:
 *       sandbox-invest-public-api.tinkoff.ru:443
 */

#include <iostream>
#include <memory>
#include <string>
#include <cstdlib>
#include <chrono>

#include "investapiclient.h"
#include "sandboxservice.h"
#include "marketdataservice.h"
#include "usersservice.h"
#include "instrumentsservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// Sandbox endpoint - sandbox tokens MUST use this endpoint
const std::string SANDBOX_HOST = "sandbox-invest-public-api.tinkoff.ru:443";

// Production endpoint - regular tokens use this
const std::string PROD_HOST = "invest-public-api.tinkoff.ru:443";

// Using declarations for enum values
using tinkoff::public_::invest::api::contract::v1::CandleInterval;
using tinkoff::public_::invest::api::contract::v1::InstrumentStatus;

// Helper function to get token from environment
std::string getToken() {
    const char* token = std::getenv("TINKOFF_TOKEN");
    if (token == nullptr || std::string(token).empty()) {
        return "";
    }
    return std::string(token);
}

bool isTokenValid() {
    std::string token = getToken();
    return !token.empty();
}

void printResult(const std::string& testName, bool success, const std::string& message = "") {
    std::cout << "[" << (success ? "PASS" : "FAIL") << "] " << testName;
    if (!message.empty()) {
        std::cout << ": " << message;
    }
    std::cout << std::endl;
}

// Check if token is a sandbox token (starts with "t.")
bool isSandboxToken(const std::string& token) {
    return token.length() > 2 && token[0] == 't' && token[1] == '.';
}

// Ensure sandbox account exists, create if needed
std::string ensureSandboxAccount(std::shared_ptr<Sandbox> sandbox) {
    std::string accountId = "";
    
    auto accountsReply = sandbox->GetSandboxAccounts();
    if (accountsReply.GetStatus().ok()) {
        auto response = std::static_pointer_cast<GetAccountsResponse>(accountsReply.ptr());
        if (response && response->accounts_size() > 0) {
            accountId = response->accounts(0).id();
            std::cout << "[INFO] Using existing sandbox account: " << accountId << std::endl;
            return accountId;
        }
    }
    
    std::cout << "[INFO] No sandbox account found, creating one..." << std::endl;
    auto openReply = sandbox->OpenSandboxAccount();
    if (openReply.GetStatus().ok()) {
            auto openResponse = std::static_pointer_cast<OpenSandboxAccountResponse>(openReply.ptr());
                if (openResponse) {
                    accountId = openResponse->account_id();
                    std::cout << "[INFO] Created new sandbox account: " << accountId << std::endl;
                }
    } else {
        std::cout << "[WARN] Failed to create sandbox account: " << openReply.GetStatus().error_message() << std::endl;
    }
    
    return accountId;
}

int main() {
    std::cout << "======================================================" << std::endl;
    std::cout << "TinkoffInvestSDK Sandbox API Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    
    std::string token = getToken();
    if (!isTokenValid()) {
        std::cout << "\nERROR: TINKOFF_TOKEN environment variable not set!" << std::endl;
        std::cout << "Please set your Tinkoff Invest API token:" << std::endl;
        std::cout << "  export TINKOFF_TOKEN=\"your_token_here\"" << std::endl;
        std::cout << "\nProvided tokens:" << std::endl;
        std::cout << "  Sandbox: t.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" << std::endl;
        std::cout << "  Prod:    t.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" << std::endl;
        return 1;
    }
    
    // Determine if this is a sandbox token and set appropriate endpoint
    bool isSandbox = isSandboxToken(token);
    std::string host = isSandbox ? SANDBOX_HOST : PROD_HOST;
    
    std::cout << "\nToken detected: " << (isSandbox ? "SANDBOX" : "PRODUCTION") << std::endl;
    std::cout << "Using endpoint: " << host << std::endl;
    std::cout << "Running tests..." << std::endl;
    
    // Create channel and services
    auto channel = grpc::CreateChannel(host, grpc::SslCredentials(grpc::SslCredentialsOptions()));
    
    auto sandbox = std::make_shared<Sandbox>(channel, token);
    auto marketdata = std::make_shared<MarketData>(channel, token);
    auto users = std::make_shared<Users>(channel, token);
    auto instruments = std::make_shared<Instruments>(channel, token);
    
    int passed = 0;
    int failed = 0;
    int skipped = 0;
    
    if (isSandbox) {
        // Ensure sandbox account exists before running sandbox tests
        std::string sandboxAccountId = ensureSandboxAccount(sandbox);
        bool hasSandboxAccount = !sandboxAccountId.empty();
        
        // SANDBOX-SPECIFIC TESTS
        std::cout << "\n--- Sandbox Tests ---" << std::endl;
        
        // Test 1: Sandbox - Open Account (only if no account exists)
        std::cout << "\n--- Test 1: Sandbox - Open Account ---" << std::endl;
        {
            if (!hasSandboxAccount) {
                auto reply = sandbox->OpenSandboxAccount();
                if (reply.GetStatus().ok()) {
                    printResult("OpenSandboxAccount", true, "Sandbox account opened");
                    passed++;
                    
                    // Clean up - close the account
                    auto closeReply = sandbox->GetSandboxAccounts();
                    if (closeReply.GetStatus().ok()) {
                        sandbox->CloseSandboxAccount(closeReply.accountID(0));
                    }
                } else {
                    printResult("OpenSandboxAccount", false, reply.GetStatus().error_message());
                    failed++;
                }
            } else {
                printResult("OpenSandboxAccount", true, "Using existing sandbox account");
                passed++;
            }
        }
        
        // Test 2: Sandbox - Get Accounts
        std::cout << "\n--- Test 2: Sandbox - Get Accounts ---" << std::endl;
        {
            auto reply = sandbox->GetSandboxAccounts();
            if (reply.GetStatus().ok()) {
                printResult("GetSandboxAccounts", true, "Found " + std::to_string(reply.accountCount()) + " sandbox account(s)");
                passed++;
            } else {
                printResult("GetSandboxAccounts", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 3: Sandbox - Deposit Funds (PayIn)
        std::cout << "\n--- Test 3: Sandbox - SandboxPayIn ---" << std::endl;
        {
            if (hasSandboxAccount) {
                auto payInReply = sandbox->SandboxPayIn(sandboxAccountId, "USD", 10000, 0);
                if (payInReply.GetStatus().ok()) {
                    printResult("SandboxPayIn", true, "Deposited 10000 USD to sandbox account");
                    passed++;
                } else {
                    printResult("SandboxPayIn", false, payInReply.GetStatus().error_message());
                    failed++;
                }
            } else {
                printResult("SandboxPayIn", false, "No sandbox account available");
                failed++;
            }
        }
        
        // Test 4: Sandbox - Get Portfolio
        std::cout << "\n--- Test 4: Sandbox - Get Portfolio ---" << std::endl;
        {
            if (hasSandboxAccount) {
                auto reply = sandbox->GetSandboxPortfolio(sandboxAccountId, PortfolioRequest::USD);
                if (reply.GetStatus().ok()) {
                    printResult("GetSandboxPortfolio", true, "Portfolio retrieved successfully");
                    passed++;
                } else {
                    printResult("GetSandboxPortfolio", false, reply.GetStatus().error_message());
                    failed++;
                }
            } else {
                printResult("GetSandboxPortfolio", false, "No sandbox account available");
                failed++;
            }
        }
        
        // Test 5: Sandbox - Get Positions
        std::cout << "\n--- Test 5: Sandbox - Get Positions ---" << std::endl;
        {
            if (hasSandboxAccount) {
                auto reply = sandbox->GetSandboxPositions(sandboxAccountId);
                if (reply.GetStatus().ok()) {
                    printResult("GetSandboxPositions", true, "Positions retrieved successfully");
                    passed++;
                } else {
                    printResult("GetSandboxPositions", false, reply.GetStatus().error_message());
                    failed++;
                }
            } else {
                printResult("GetSandboxPositions", false, "No sandbox account available");
                failed++;
            }
        }
        
        // Test 6: Sandbox - Close Account
        std::cout << "\n--- Test 6: Sandbox - Close Account ---" << std::endl;
        {
            auto accountsReply = sandbox->GetSandboxAccounts();
            if (accountsReply.GetStatus().ok() && accountsReply.accountCount() > 0) {
                std::string accountToClose = accountsReply.accountID(0);
                auto closeReply = sandbox->CloseSandboxAccount(accountToClose);
                if (closeReply.GetStatus().ok()) {
                    printResult("CloseSandboxAccount", true, "Sandbox account closed");
                    passed++;
                } else {
                    printResult("CloseSandboxAccount", false, closeReply.GetStatus().error_message());
                    failed++;
                }
            } else {
                printResult("CloseSandboxAccount", false, "No sandbox account to close");
                failed++;
            }
        }
        
        std::cout << "\n[INFO] Sandbox token tests completed." << std::endl;
        std::cout << "[INFO] Note: Users/MarketData services are not available with sandbox tokens." << std::endl;
    }
    else {
        // PRODUCTION TOKEN TESTS
        
        // Test 1: Get User Info
        std::cout << "\n--- Test 1: Get User Info ---" << std::endl;
        {
            auto reply = users->GetInfo();
            if (reply.GetStatus().ok()) {
                printResult("GetInfo", true, "User info retrieved successfully");
                passed++;
            } else {
                printResult("GetInfo", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 2: Get Accounts
        std::cout << "\n--- Test 2: Get Accounts ---" << std::endl;
        {
            auto reply = users->GetAccounts();
            if (reply.GetStatus().ok()) {
                printResult("GetAccounts", true, "Found " + std::to_string(reply.accountCount()) + " account(s)");
                passed++;
            } else {
                printResult("GetAccounts", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 3: Get Last Prices
        std::cout << "\n--- Test 3: Get Last Prices ---" << std::endl;
        {
            std::vector<std::string> instrumentsList = {"BBG004S68104"}; // Sberbank
            auto reply = marketdata->GetLastPrices(instrumentsList);
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<GetLastPricesResponse>(reply.ptr());
                if (response) {
                    printResult("GetLastPrices", true, "Retrieved " + std::to_string(response->last_prices_size()) + " prices");
                    passed++;
                } else {
                    printResult("GetLastPrices", false, "Could not cast response");
                    failed++;
                }
            } else {
                printResult("GetLastPrices", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 4: Get Order Book
        std::cout << "\n--- Test 4: Get Order Book ---" << std::endl;
        {
            auto reply = marketdata->GetOrderBook("BBG004S68104", 10);
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<GetOrderBookResponse>(reply.ptr());
                if (response) {
                    printResult("GetOrderBook", true, "Depth: " + std::to_string(response->bids_size()) + " bids, " + std::to_string(response->asks_size()) + " asks");
                    passed++;
                } else {
                    printResult("GetOrderBook", false, "Could not cast response");
                    failed++;
                }
            } else {
                printResult("GetOrderBook", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 5: Get Candles
        std::cout << "\n--- Test 5: Get Candles ---" << std::endl;
        {
            auto now = std::chrono::system_clock::now();
            auto yesterday = now - std::chrono::hours(24);
            auto now_ts = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
            auto yesterday_ts = std::chrono::duration_cast<std::chrono::seconds>(yesterday.time_since_epoch()).count();
            
            auto reply = marketdata->GetCandles("BBG004S68104", yesterday_ts, 0, now_ts, 0, CANDLE_INTERVAL_HOUR);
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<GetCandlesResponse>(reply.ptr());
                if (response) {
                    printResult("GetCandles", true, "Retrieved " + std::to_string(response->candles_size()) + " candles");
                    passed++;
                } else {
                    printResult("GetCandles", false, "Could not cast response");
                    failed++;
                }
            } else {
                printResult("GetCandles", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 6: Get Trading Status
        std::cout << "\n--- Test 6: Get Trading Status ---" << std::endl;
        {
            auto reply = marketdata->GetTradingStatus("BBG004S68104");
            if (reply.GetStatus().ok()) {
                printResult("GetTradingStatus", true, "Trading status retrieved");
                passed++;
            } else {
                printResult("GetTradingStatus", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 7: Get Shares
        std::cout << "\n--- Test 7: Get Shares ---" << std::endl;
        {
            auto reply = instruments->Shares(INSTRUMENT_STATUS_BASE);
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<SharesResponse>(reply.ptr());
                if (response) {
                    printResult("GetShares", true, "Retrieved " + std::to_string(response->instruments_size()) + " shares");
                    passed++;
                } else {
                    printResult("GetShares", false, "Could not cast response");
                    failed++;
                }
            } else {
                printResult("GetShares", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 8: Sandbox - Open Account
        std::cout << "\n--- Test 8: Sandbox - Open Account ---" << std::endl;
        {
            auto reply = sandbox->OpenSandboxAccount();
            if (reply.GetStatus().ok()) {
                printResult("OpenSandboxAccount", true, "Sandbox account opened");
                passed++;
                
                // Clean up - close the account
                auto closeReply = sandbox->GetSandboxAccounts();
                if (closeReply.GetStatus().ok() && closeReply.accountCount() > 0) {
                    sandbox->CloseSandboxAccount(closeReply.accountID(0));
                }
            } else {
                printResult("OpenSandboxAccount", false, reply.GetStatus().error_message());
                failed++;
            }
        }
        
        // Test 9: Sandbox - Get Accounts
        std::cout << "\n--- Test 9: Sandbox - Get Accounts ---" << std::endl;
        {
            auto reply = sandbox->GetSandboxAccounts();
            if (reply.GetStatus().ok()) {
                printResult("GetSandboxAccounts", true, "Found " + std::to_string(reply.accountCount()) + " sandbox account(s)");
                passed++;
            } else {
                printResult("GetSandboxAccounts", false, reply.GetStatus().error_message());
                failed++;
            }
        }
    }
    
    // Summary
    std::cout << "\n======================================================" << std::endl;
    std::cout << "Test Results: " << passed << " passed, " << failed << " failed, " << skipped << " skipped" << std::endl;
    std::cout << "======================================================" << std::endl;
    
    return failed > 0 ? 1 : 0;
}

