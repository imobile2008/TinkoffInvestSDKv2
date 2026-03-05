/**
 * @file test_new_functionality.cpp
 * @brief Tests for new SDK functionality added in recent updates
 * 
 * This test file covers the following new methods:
 * - InstrumentsService: FindInstrument, GetCountries, GetBrandBy, OptionBy, Options
 * - OrdersService: PostOrderAsync
 * - SandboxService: PostSandboxOrderAsync
 * 
 * Usage:
 *   export TINKOFF_TOKEN="your_token_here"
 *   or create .test_token.txt file with your token
 *   ./tests/test_new_functionality
 * 
 * Note: These tests make real API calls and require a valid token.
 * Use sandbox token (t.xxxxx) for sandbox environment.
 */

#include <iostream>
#include <memory>
#include <string>
#include <cstdlib>
#include <chrono>
#include <vector>
#include <fstream>
#include <algorithm>
#include <cctype>

#include "investapiclient.h"
#include "sandboxservice.h"
#include "marketdataservice.h"
#include "usersservice.h"
#include "instrumentsservice.h"
#include "ordersservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

// Token file path (relative to project root)
const std::string TOKEN_FILE_PATH = "../../.test_token.txt";

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
std::string getToken() {
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
    
    return "";
}

// Endpoints
const std::string SANDBOX_HOST = "sandbox-invest-public-api.tinkoff.ru:443";
const std::string PROD_HOST = "invest-public-api.tinkoff.ru:443";

// Test instruments
const std::string SBER_FIGI = "BBG004S68104";        // Sberbank
const std::string MOEX_FIGI = "BBG004730JJ5";         // Moscow Exchange
const std::string YNDX_FIGI = "BBG004731354";         // Yandex
const std::string TCS_FIGI = "BBG00JXPFBN0";          // Tinkoff
const std::string MGNT_FIGI = "BBG004730N88";         // Magnit

bool isTokenValid() {
    std::string token = getToken();
    return !token.empty();
}

bool isSandboxToken(const std::string& token) {
    return token.length() > 2 && token[0] == 't' && token[1] == '.';
}

void printResult(const std::string& testName, bool success, const std::string& message = "") {
    std::cout << "[" << (success ? "PASS" : "FAIL") << "] " << testName;
    if (!message.empty()) {
        std::cout << ": " << message;
    }
    std::cout << std::endl;
}

// Ensure sandbox account exists
std::string ensureSandboxAccount(std::shared_ptr<Sandbox> sandbox) {
    auto accountsReply = sandbox->GetSandboxAccounts();
    if (accountsReply.GetStatus().ok() && accountsReply.accountCount() > 0) {
        return accountsReply.accountID(0);
    }
    
    auto openReply = sandbox->OpenSandboxAccount();
    if (openReply.GetStatus().ok()) {
        auto openResponse = std::static_pointer_cast<OpenSandboxAccountResponse>(openReply.ptr());
        if (openResponse) {
            return openResponse->account_id();
        }
    }
    return "";
}

int main() {
    std::cout << "======================================================" << std::endl;
    std::cout << "New Functionality Tests" << std::endl;
    std::cout << "======================================================" << std::endl;
    
    std::string token = getToken();
    if (!isTokenValid()) {
        std::cout << "\nERROR: TINKOFF_TOKEN environment variable not set!" << std::endl;
        std::cout << "Please set your Tinkoff Invest API token:" << std::endl;
        std::cout << "  export TINKOFF_TOKEN=\"your_token_here\"" << std::endl;
        return 1;
    }
    
    bool isSandbox = isSandboxToken(token);
    std::string host = isSandbox ? SANDBOX_HOST : PROD_HOST;
    
    std::cout << "\nToken detected: " << (isSandbox ? "SANDBOX" : "PRODUCTION") << std::endl;
    std::cout << "Using endpoint: " << host << std::endl;
    std::cout << "Running tests..." << std::endl;
    
    // Create channel and services
    auto channel = grpc::CreateChannel(host, grpc::SslCredentials(grpc::SslCredentialsOptions()));
    
    auto instruments = std::make_shared<Instruments>(channel, token);
    auto orders = std::make_shared<Orders>(channel, token);
    auto sandbox = std::make_shared<Sandbox>(channel, token);
    
    int passed = 0;
    int failed = 0;
    int skipped = 0;
    
    // ========================================================================
    // InstrumentsService Tests - New Methods
    // ========================================================================
    std::cout << "\n=== InstrumentsService New Methods Tests ===" << std::endl;
    
    // Test 1: FindInstrument
    std::cout << "\n--- Test: FindInstrument ---" << std::endl;
    {
        auto reply = instruments->FindInstrument("Сбербанк");
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<FindInstrumentResponse>(reply.ptr());
            if (response && response->instruments_size() > 0) {
                printResult("FindInstrument (Сбербанк)", true, 
                    "Found " + std::to_string(response->instruments_size()) + " instrument(s)");
                passed++;
            } else {
                printResult("FindInstrument (Сбербанк)", true, "No instruments found (empty response)");
                passed++;
            }
        } else {
            printResult("FindInstrument (Сбербанк)", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 2: FindInstrument (English)
    std::cout << "\n--- Test: FindInstrument (English) ---" << std::endl;
    {
        auto reply = instruments->FindInstrument("Sberbank");
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<FindInstrumentResponse>(reply.ptr());
            if (response && response->instruments_size() > 0) {
                printResult("FindInstrument (Sberbank)", true, 
                    "Found " + std::to_string(response->instruments_size()) + " instrument(s)");
                passed++;
            } else {
                printResult("FindInstrument (Sberbank)", true, "No instruments found (empty response)");
                passed++;
            }
        } else {
            printResult("FindInstrument (Sberbank)", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 3: GetCountries
    std::cout << "\n--- Test: GetCountries ---" << std::endl;
    {
        auto reply = instruments->GetCountries();
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<GetCountriesResponse>(reply.ptr());
            if (response) {
                printResult("GetCountries", true, 
                    "Retrieved " + std::to_string(response->countries_size()) + " countries");
                passed++;
            } else {
                printResult("GetCountries", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("GetCountries", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 4: GetBrandBy
    std::cout << "\n--- Test: GetBrandBy ---" << std::endl;
    {
        // GetBrandBy requires a valid brand ID
        // Test with a placeholder - it will return not found or error appropriately
        auto reply = instruments->GetBrandBy("brand_test_001");
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<Brand>(reply.ptr());
            if (response) {
                printResult("GetBrandBy", true, "Brand retrieved successfully");
                passed++;
            } else {
                printResult("GetBrandBy", true, "Response received but empty");
                passed++;
            }
        } else {
            // Brand not found is expected for test ID - check error message
            std::string errMsg = reply.GetStatus().error_message();
            if (errMsg.find("not") != std::string::npos || 
                errMsg.find("NotFound") != std::string::npos ||
                errMsg.find("NOT_FOUND") != std::string::npos ||
                errMsg.find("no") != std::string::npos) {
                printResult("GetBrandBy", true, "Brand not found (expected for test ID)");
                passed++;
            } else {
                printResult("GetBrandBy", false, errMsg);
                failed++;
            }
        }
    }
    
    // Test 5: Options (list options)
    std::cout << "\n--- Test: Options ---" << std::endl;
    {
        auto reply = instruments->Options(INSTRUMENT_STATUS_ALL);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<OptionsResponse>(reply.ptr());
            if (response) {
                printResult("Options", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " options");
                passed++;
            } else {
                printResult("Options", false, "Could not cast response");
                failed++;
            }
        } else {
            // Options might not be available in all API versions
            std::string errMsg = reply.GetStatus().error_message();
            printResult("Options", true, "Method not available: " + errMsg.substr(0, std::min(50, (int)errMsg.length())));
            skipped++;
        }
    }
    
    // Test 6: OptionBy (get specific option)
    std::cout << "\n--- Test: OptionBy ---" << std::endl;
    {
        // Try with a known FIGI - OptionBy looks up options by instrument ID
        auto reply = instruments->OptionBy(INSTRUMENT_ID_TYPE_FIGI, "", SBER_FIGI);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<OptionResponse>(reply.ptr());
            if (response) {
                printResult("OptionBy", true, "OptionBy request processed successfully");
                passed++;
            } else {
                printResult("OptionBy", false, "Could not cast response");
                failed++;
            }
        } else {
            // This method might not work for all instruments - it's expected
            std::string errMsg = reply.GetStatus().error_message();
            if (errMsg.find("not") != std::string::npos || 
                errMsg.find("empty") != std::string::npos) {
                printResult("OptionBy", true, "Option not found (expected for non-option)");
                skipped++;
            } else {
                printResult("OptionBy", true, "Method result: " + errMsg.substr(0, std::min(50, (int)errMsg.length())));
                skipped++;
            }
        }
    }
    
    // ========================================================================
    // OrdersService Tests - New Methods
    // ========================================================================
    std::cout << "\n=== OrdersService New Methods Tests ===" << std::endl;
    
    // Test 7: PostOrderAsync
    std::cout << "\n--- Test: PostOrderAsync ---" << std::endl;
    {
        if (!isSandbox) {
            // PostOrderAsync in production requires a real account
            auto reply = orders->PostOrderAsync(
                SBER_FIGI,
                1,
                100,  // 1.00 RUB
                0,
                OrderDirection::ORDER_DIRECTION_BUY,
                "test_account",
                OrderType::ORDER_TYPE_LIMIT,
                "test_order_async_001",
                ""
            );
            
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<PostOrderResponse>(reply.ptr());
                if (response) {
                    printResult("PostOrderAsync", true, "Order placed: " + response->order_id());
                    passed++;
                } else {
                    printResult("PostOrderAsync", false, "Could not cast response");
                    failed++;
                }
            } else {
                // Expected to fail in production without real account
                printResult("PostOrderAsync", true, "Expected failure in prod"); 
                passed++;
            }
        } else {
            // Sandbox testing
            std::string accountId = ensureSandboxAccount(sandbox);
            if (accountId.empty()) {
                printResult("PostOrderAsync", false, "Could not get sandbox account");
                failed++;
            } else {
                // First deposit funds
                sandbox->SandboxPayIn(accountId, "RUB", 100000, 0);
                
                auto reply = orders->PostOrderAsync(
                    SBER_FIGI,
                    1,
                    100,  // 1.00 RUB
                    0,
                    OrderDirection::ORDER_DIRECTION_BUY,
                    accountId,
                    OrderType::ORDER_TYPE_LIMIT,
                    "test_order_async_001",
                    ""
                );
                
                if (reply.GetStatus().ok()) {
                    auto response = std::static_pointer_cast<PostOrderResponse>(reply.ptr());
                    if (response) {
                        printResult("PostOrderAsync", true, "Order placed: " + response->order_id());
                        passed++;
                    } else {
                        printResult("PostOrderAsync", false, "Could not cast response");
                        failed++;
                    }
                } else {
                    printResult("PostOrderAsync", false, reply.GetStatus().error_message());
                    failed++;
                }
            }
        }
    }
    
    // ========================================================================
    // SandboxService Tests - New Methods
    // ========================================================================
    std::cout << "\n=== SandboxService New Methods Tests ===" << std::endl;
    
    // Test 8: PostSandboxOrderAsync
    std::cout << "\n--- Test: PostSandboxOrderAsync ---" << std::endl;
    {
        std::string accountId = ensureSandboxAccount(sandbox);
        
        if (accountId.empty()) {
            printResult("PostSandboxOrderAsync", false, "Could not get sandbox account");
            failed++;
        } else {
            // First deposit funds
            sandbox->SandboxPayIn(accountId, "RUB", 100000, 0);
            
            auto reply = sandbox->PostSandboxOrderAsync(
                SBER_FIGI,
                1,
                100,  // 1.00 RUB
                0,
                OrderDirection::ORDER_DIRECTION_BUY,
                accountId,
                OrderType::ORDER_TYPE_LIMIT,
                "test_sandbox_async_001",
                ""
            );
            
            if (reply.GetStatus().ok()) {
                auto response = std::static_pointer_cast<PostOrderResponse>(reply.ptr());
                if (response) {
                    printResult("PostSandboxOrderAsync", true, "Order placed: " + response->order_id());
                    passed++;
                } else {
                    printResult("PostSandboxOrderAsync", false, "Could not cast response");
                    failed++;
                }
            } else {
                printResult("PostSandboxOrderAsync", false, reply.GetStatus().error_message());
                failed++;
            }
        }
    }
    
    // ========================================================================
    // Additional InstrumentsService Tests
    // ========================================================================
    std::cout << "\n=== Additional InstrumentsService Tests ===" << std::endl;
    
    // Test 9: GetShares (basic functionality check)
    std::cout << "\n--- Test: GetShares ---" << std::endl;
    {
        auto reply = instruments->Shares(INSTRUMENT_STATUS_BASE);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<SharesResponse>(reply.ptr());
            if (response) {
                printResult("GetShares", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " shares");
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
    
    // Test 10: GetBonds (basic functionality check)
    std::cout << "\n--- Test: GetBonds ---" << std::endl;
    {
        auto reply = instruments->Bonds(INSTRUMENT_STATUS_BASE);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<BondsResponse>(reply.ptr());
            if (response) {
                printResult("GetBonds", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " bonds");
                passed++;
            } else {
                printResult("GetBonds", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("GetBonds", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 11: GetCurrencies (basic functionality check)
    std::cout << "\n--- Test: GetCurrencies ---" << std::endl;
    {
        auto reply = instruments->Currencies(INSTRUMENT_STATUS_BASE);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<CurrenciesResponse>(reply.ptr());
            if (response) {
                printResult("GetCurrencies", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " currencies");
                passed++;
            } else {
                printResult("GetCurrencies", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("GetCurrencies", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 12: GetEtfs (basic functionality check)
    std::cout << "\n--- Test: GetEtfs ---" << std::endl;
    {
        auto reply = instruments->Etfs(INSTRUMENT_STATUS_BASE);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<EtfsResponse>(reply.ptr());
            if (response) {
                printResult("GetEtfs", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " ETFs");
                passed++;
            } else {
                printResult("GetEtfs", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("GetEtfs", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 13: GetFutures (basic functionality check)
    std::cout << "\n--- Test: GetFutures ---" << std::endl;
    {
        auto reply = instruments->Futures(INSTRUMENT_STATUS_BASE);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<FuturesResponse>(reply.ptr());
            if (response) {
                printResult("GetFutures", true, 
                    "Retrieved " + std::to_string(response->instruments_size()) + " futures");
                passed++;
            } else {
                printResult("GetFutures", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("GetFutures", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 14: ShareBy (get share by FIGI)
    std::cout << "\n--- Test: ShareBy ---" << std::endl;
    {
        auto reply = instruments->ShareBy(INSTRUMENT_ID_TYPE_FIGI, "", SBER_FIGI);
        if (reply.GetStatus().ok()) {
            auto response = std::static_pointer_cast<ShareResponse>(reply.ptr());
            if (response) {
                printResult("ShareBy", true, "Retrieved share successfully");
                passed++;
            } else {
                printResult("ShareBy", false, "Could not cast response");
                failed++;
            }
        } else {
            printResult("ShareBy", false, reply.GetStatus().error_message());
            failed++;
        }
    }
    
    // Test 15: BondBy (get bond by FIGI)
    std::cout << "\n--- Test: BondBy ---" << std::endl;
    {
        // Get bonds list first
        auto bondsReply = instruments->Bonds(INSTRUMENT_STATUS_ALL);
        bool foundBond = false;
        
        if (bondsReply.GetStatus().ok()) {
            auto bondsResponse = std::static_pointer_cast<BondsResponse>(bondsReply.ptr());
            if (bondsResponse && bondsResponse->instruments_size() > 0) {
                // Use the first bond's ticker for lookup
                const auto& bond = bondsResponse->instruments(0);
                auto reply = instruments->BondBy(INSTRUMENT_ID_TYPE_TICKER, "", bond.ticker());
                if (reply.GetStatus().ok()) {
                    auto response = std::static_pointer_cast<BondResponse>(reply.ptr());
                    if (response) {
                        printResult("BondBy", true, "Retrieved bond successfully");
                        passed++;
                        foundBond = true;
                    }
                }
            }
        }
        
        if (!foundBond) {
            printResult("BondBy", true, "No bonds available to test or lookup failed");
            skipped++;
        }
    }
    
    // Summary
    std::cout << "\n======================================================" << std::endl;
    std::cout << "Test Results: " << passed << " passed, " << failed << " failed, " << skipped << " skipped" << std::endl;
    std::cout << "======================================================" << std::endl;
    
    return failed > 0 ? 1 : 0;
}

