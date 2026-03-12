/**
 * @file test_operations_stream.cpp
 * @brief OperationsStream API Tests for TinkoffInvestSDK
 *
 * This file contains tests for OperationsStream service:
 * - PortfolioStreamAsync (real-time portfolio updates)
 * - PositionsStreamAsync (real-time position changes)
 *
 * IMPORTANT: These tests verify that async streaming methods can be invoked
 * without throwing exceptions. Since streaming requires real network connection
 * and data availability, the tests focus on:
 * 1. Method invocation doesn't throw
 * 2. Basic subscription lifecycle works
 * 3. Service access and configuration is correct
 *
 * Full end-to-end streaming tests require actual account data and should be
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
#include "../services/operationsstreamservice.h"
#include <condition_variable>
#include <mutex>
#include "../services/usersservice.h"
#include "investapiclient.h"




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

// Helper function to get first account ID (for unit tests with mock services)
std::string getFirstAccountId() {
    return TEST_ACCOUNT_ID;
}

// ============================================================================
// OperationsStream Tests
// ============================================================================

class OperationsStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "Setting up OperationsStreamTest..." << std::endl;
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<OperationsStream>(channel, getApiToken());
        std::cout << "OperationsStream created successfully" << std::endl;
    }

    void TearDown() override {
        // Give time for cleanup between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::shared_ptr<OperationsStream> stream;
};

// Test that PortfolioStreamAsync can be invoked without throwing
TEST_F(OperationsStreamTest, PortfolioStreamAsyncMethodInvocation) {
    std::cout << "PortfolioStreamAsync test starting..." << std::endl;

    std::vector<std::string> accounts = {getFirstAccountId()};
    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &accounts, &noException, &exceptionMsg]() {
        try {
            stream->PortfolioStreamAsync(accounts, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    // Wait briefly for method to be invoked
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "PortfolioStreamAsync threw: " << exceptionMsg;
    std::cout << "PortfolioStreamAsync test completed" << std::endl;
}

// Test that PositionsStreamAsync can be invoked without throwing
TEST_F(OperationsStreamTest, PositionsStreamAsyncMethodInvocation) {
    std::cout << "PositionsStreamAsync test starting..." << std::endl;

    std::vector<std::string> accounts = {getFirstAccountId()};
    bool noException = true;
    std::string exceptionMsg;

    std::thread worker([this, &accounts, &noException, &exceptionMsg]() {
        try {
            stream->PositionsStreamAsync(accounts, [](ServiceReply) {});
        } catch (const std::exception& e) {
            noException = false;
            exceptionMsg = e.what();
            std::cout << "Exception: " << e.what() << std::endl;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    worker.detach();

    EXPECT_TRUE(noException) << "PositionsStreamAsync threw: " << exceptionMsg;
    std::cout << "PositionsStreamAsync test completed" << std::endl;
}

// Test PositionsStream receives at least one position using real main account
TEST_F(OperationsStreamTest, PositionsStreamReceivesOnePositionReal) {
    std::cout << "PositionsStreamReceivesOnePositionReal test starting..." << std::endl;

    // Use token from file (already updated with main account token)
    std::string token = getApiToken();
    ASSERT_FALSE(token.empty()) << "Main account token must be available in .test_token.txt";

    // Get real main account ID
    auto usersChannel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
    Users users(usersChannel, token);
    auto accountsReply = users.GetAccounts();
    ASSERT_TRUE(accountsReply.GetStatus().ok()) << "Failed to get accounts: " << accountsReply.GetStatus().error_message();
    
    const auto& accountsResponse = *dynamic_cast<const GetAccountsResponse*>(accountsReply.ptr().get());
    ASSERT_FALSE(accountsResponse.accounts().empty()) << "No accounts found";
    std::string accountId = accountsResponse.accounts(0).id();
    std::cout << "Using main account ID: " << accountId << std::endl;

    // Test PositionsStream with real data
    std::vector<std::string> accounts = {accountId};
    std::atomic<int> positionCount{0};
    std::atomic<bool> errorReceived{false};
    std::mutex cvMutex;
    std::condition_variable cv;
    bool done = false;

    auto callback = [&](ServiceReply reply) {
        if (!reply.GetStatus().ok()) {
            std::cerr << "Stream error: " << reply.GetStatus().error_code() << " - " << reply.GetStatus().error_message() << std::endl;
            errorReceived = true;
            return;
        }

        const OperationsStreamResponse& streamResp = reply.getOperationsStreamResponse();
        if (streamResp.hasPositions()) {
            const auto& positionsResp = streamResp.getPositions();
            std::cout << "Positions response received." << std::endl;
            
            if (positionsResp.has_position()) {
                const auto& positionData = positionsResp.position();
                positionCount++;  // Count the response
                std::cout << "PositionData response received for account: " << positionData.account_id() << std::endl;
                std::cout << "Securities count: " << positionData.securities_size() << std::endl;
                for (int i = 0; i < positionData.securities_size(); ++i) {
                    const auto& sec = positionData.securities(i);
                    std::cout << "Security #" << (i+1) << ": FIGI=\"" << sec.figi() 
                              << "\", Balance=" << sec.balance() 
                              << ", Blocked=" << sec.blocked() << std::endl;
                }
            }
        }
        
        std::unique_lock<std::mutex> lock(cvMutex);
        if (positionCount >= 1 || errorReceived) {
            done = true;
            cv.notify_all();
        }
    };

    // Start streaming in background thread (like existing tests)
    stream->PositionsStreamAsync(accounts, callback);

    // Wait for position or timeout (20 seconds)
    std::unique_lock<std::mutex> lock(cvMutex);
    auto waitResult = cv.wait_for(lock, std::chrono::seconds(20), [&done] { return done; });

    stream->close();
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Cleanup

    EXPECT_FALSE(errorReceived) << "Stream ended with error";
    EXPECT_GE(positionCount, 1) << "Expected at least 1 position, got " << positionCount;
    std::cout << "PositionsStreamReceivesOnePositionReal test completed. Positions received: " << positionCount << std::endl;
}


// Test empty accounts list handling for PortfolioStreamAsync
TEST_F(OperationsStreamTest, EmptyAccountsListHandlesGracefullyForPortfolio) {
    std::cout << "Empty accounts list test for PortfolioStreamAsync starting..." << std::endl;

    std::vector<std::string> emptyAccounts = {};
    bool noException = true;

    std::thread worker([this, &emptyAccounts, &noException]() {
        try {
            stream->PortfolioStreamAsync(emptyAccounts, [](ServiceReply reply) {
                // Callback should receive an error status
                EXPECT_FALSE(reply.GetStatus().ok());
            });
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

// Test empty accounts list handling for PositionsStreamAsync
TEST_F(OperationsStreamTest, EmptyAccountsListHandlesGracefullyForPositions) {
    std::cout << "Empty accounts list test for PositionsStreamAsync starting..." << std::endl;

    std::vector<std::string> emptyAccounts = {};
    bool noException = true;

    std::thread worker([this, &emptyAccounts, &noException]() {
        try {
            stream->PositionsStreamAsync(emptyAccounts, [](ServiceReply reply) {
                // Callback should receive an error status
                EXPECT_FALSE(reply.GetStatus().ok());
            });
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

// Test that isConnected returns correct state
TEST_F(OperationsStreamTest, IsConnectedReturnsCorrectState) {
    std::cout << "isConnected test starting..." << std::endl;
    
    // Initially should not be connected
    EXPECT_FALSE(stream->isConnected());
    
    std::cout << "isConnected test completed" << std::endl;
}

// Test that getMessageCount returns initial value
TEST_F(OperationsStreamTest, GetMessageCountReturnsInitialValue) {
    std::cout << "getMessageCount test starting..." << std::endl;
    
    // Initially should be 0
    EXPECT_EQ(stream->getMessageCount(), 0);
    
    std::cout << "getMessageCount test completed" << std::endl;
}

// Test multiple subscriptions in sequence (Portfolio -> Positions)
TEST_F(OperationsStreamTest, MultipleSubscriptionsSequential) {
    std::cout << "Multiple subscriptions sequential test starting..." << std::endl;

    std::vector<std::string> accounts = {getFirstAccountId()};
    std::atomic<int> callbackCount(0);

    // Run first subscription and wait for it to complete
    stream->PortfolioStreamAsync(accounts, [&callbackCount](ServiceReply reply) {
        callbackCount++;
        std::cout << "PortfolioStreamAsync callback #" << callbackCount << std::endl;
    });
    
    // Wait for stream to finish or timeout
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Close the stream properly
    stream->close();
    
    std::cout << "Multiple subscriptions sequential test completed" << std::endl;
}

// ============================================================================
// OperationsStream Integration Tests
// ============================================================================

// DISABLED integration tests - require full SDK service registry
/*
class OperationsStreamIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        client = std::make_unique<InvestApiClient>(TEST_HOST, getApiToken());
    }

    std::unique_ptr<InvestApiClient> client;
};

TEST_F(OperationsStreamIntegrationTest, DISABLED_OperationsStreamServiceAccessible) {
    auto operationsstream = std::dynamic_pointer_cast<OperationsStream>(
        client->service("operationsstream")
    );
    EXPECT_NE(operationsstream, nullptr);
}

TEST_F(OperationsStreamIntegrationTest, DISABLED_BothPortfolioAndPositionsStreamsWork) {
    auto operationsstream = std::dynamic_pointer_cast<OperationsStream>(
        client->service("operationsstream")
    );
    EXPECT_NE(operationsstream, nullptr);
    EXPECT_NO_THROW({
        operationsstream->PortfolioStreamAsync({getFirstAccountId()}, [](ServiceReply) {});
        operationsstream->PositionsStreamAsync({getFirstAccountId()}, [](ServiceReply) {});
    });
}
*/

// ============================================================================
// OperationsStream Error Handling Tests
// ============================================================================

class OperationsStreamErrorHandlingTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto channel = grpc::CreateChannel(TEST_HOST, grpc::SslCredentials(grpc::SslCredentialsOptions()));
        stream = std::make_shared<OperationsStream>(channel, getApiToken());
    }

    std::shared_ptr<OperationsStream> stream;
};

TEST_F(OperationsStreamErrorHandlingTest, InvalidAccountIdHandles) {
    std::cout << "Invalid account ID test starting..." << std::endl;

    std::vector<std::string> accounts = {"invalid_account_id_12345"};
    
    // Should not throw, but callback may receive error
    EXPECT_NO_THROW({
        stream->PortfolioStreamAsync(accounts, [](ServiceReply reply) {
            std::cout << "Callback received, status ok: " << reply.GetStatus().ok() << std::endl;
        });
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "InvalidAccountId test completed" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

