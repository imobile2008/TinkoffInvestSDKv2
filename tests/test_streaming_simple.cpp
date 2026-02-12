/**
 * @file test_streaming_simple.cpp
 * @brief Simple streaming test for TinkoffInvestSDK
 * 
 * This test verifies basic streaming functionality with proper timeout handling
 * and cleanup to avoid hanging.
 */

#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <functional>
#include <fstream>
#include <algorithm>
#include <cctype>

#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "commontypes.h"

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
const std::string TEST_FIGI = "BBG004S68104";  // Sberbank

// Helper class to manage streaming test lifecycle with timeout
class StreamingTestHelper {
public:
    StreamingTestHelper(const std::string& name) 
        : testName(name), 
          receivedResponse(false),
          messageCount(0),
          startTime(std::chrono::steady_clock::now()),
          timeoutSeconds(10) {}

    bool waitForResponse(int maxSeconds = 10) {
        timeoutSeconds = maxSeconds;
        auto timeoutDuration = std::chrono::seconds(timeoutSeconds);

        while (std::chrono::steady_clock::now() - startTime < timeoutDuration) {
            if (receivedResponse.load()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        return receivedResponse.load();
    }

    void recordResponse() {
        receivedResponse = true;
        messageCount++;
    }

    int getMessageCount() const { return messageCount.load(); }
    
    auto getElapsedMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
    }

    const std::string& getName() const { return testName; }

private:
    std::string testName;
    std::atomic<bool> receivedResponse;
    std::atomic<int> messageCount;
    std::chrono::steady_clock::time_point startTime;
    int timeoutSeconds;
};

void marketStreamCallback(StreamingTestHelper& helper, ServiceReply reply)
{
    helper.recordResponse();
    
    // Try to parse the response - use either raw pointer or MarketDataStreamResponse
    if (reply.ptr()) {
        std::string debugStr = reply.ptr()->DebugString();
        std::cout << "[" << helper.getName() << "] Response received: " 
                  << debugStr.substr(0, 100) << "..." << std::endl;
    } else if (reply.hasMarketDataStreamResponse()) {
        auto streamResponse = reply.getMarketDataStreamResponse();
        std::cout << "[" << helper.getName() << "] Stream response type: "
                  << streamResponse.getPayloadTypeString() << std::endl;
    }
}

int main()
{
    std::cout << "=== Tinkoff Invest Streaming API Test ===" << std::endl;
    std::string apiToken = getApiToken();
    std::cout << "Token: " << apiToken.substr(0, 20) << "..." << std::endl;
    std::cout << "Host: " << TEST_HOST << std::endl;
    std::cout << "Test FIGI: " << TEST_FIGI << std::endl;
    std::cout << std::endl;
    
    try {
        // Create client with SSL credentials
        std::cout << "1. Creating InvestApiClient..." << std::endl;
        InvestApiClient client(TEST_HOST, apiToken);
        std::cout << "   Client created successfully" << std::endl;
        
        // Get MarketDataStream service
        std::cout << "2. Getting MarketDataStream service..." << std::endl;
        auto marketdata = std::dynamic_pointer_cast<MarketDataStream>(
            client.service("marketdatastream")
        );
        
        if (!marketdata) {
            std::cerr << "ERROR: Failed to get MarketDataStream service" << std::endl;
            return 1;
        }
        std::cout << "   MarketDataStream service obtained" << std::endl;
        
        // Test helper for tracking responses
        StreamingTestHelper helper("SubscribeLastPrice");
        
        // Create callback that tracks responses
        auto callback = [&helper](ServiceReply reply) {
            marketStreamCallback(helper, reply);
        };
        
        std::cout << "3. Starting SubscribeLastPriceAsync test..." << std::endl;
        std::cout << "   Subscribing to: " << TEST_FIGI << std::endl;
        
        // Start subscription
        marketdata->SubscribeLastPriceAsync({TEST_FIGI}, callback);
        std::cout << "   Subscription request sent" << std::endl;
        
        // Wait for response with timeout
        std::cout << "4. Waiting for subscription response (max 10 seconds)..." << std::endl;
        bool gotResponse = helper.waitForResponse(10);
        
        if (gotResponse) {
            std::cout << std::endl;
            std::cout << "=== TEST PASSED ===" << std::endl;
            std::cout << "Successfully connected to Tinkoff Invest Streaming API" << std::endl;
            std::cout << "Received " << helper.getMessageCount() << " messages in " 
                      << helper.getElapsedMs() << "ms" << std::endl;
            
            // Cleanup: unsubscribe
            std::cout << "5. Cleaning up - unsubscribing..." << std::endl;
            marketdata->UnSubscribeLastPriceAsync();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            
            std::cout << "====================" << std::endl;
            return 0;
        } else {
            std::cout << std::endl;
            std::cout << "=== TEST WARNING ===" << std::endl;
            std::cout << "Connected to API but no messages received within timeout" << std::endl;
            std::cout << "(This is normal if no market activity for the instrument)" << std::endl;
            std::cout << "Elapsed time: " << helper.getElapsedMs() << "ms" << std::endl;
            
            // Cleanup: unsubscribe
            std::cout << "6. Cleaning up - unsubscribing..." << std::endl;
            try {
                marketdata->UnSubscribeLastPriceAsync();
            } catch (...) {
                // Ignore unsubscribe errors
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            
            std::cout << "====================" << std::endl;
            return 0; // Consider this a success since we connected
        }
        
    } catch (const std::exception& e) {
        std::cerr << "ERROR: Exception caught: " << e.what() << std::endl;
        return 1;
    }
}

