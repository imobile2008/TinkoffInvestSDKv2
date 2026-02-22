#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

// Helper function to load token from file with fallback to environment variable
std::string loadToken(const std::string& filename = ".test_token.txt")
{
    // First try to load from file
    std::ifstream file(filename);
    if (file.is_open()) {
        std::string token;
        std::getline(file, token);
        // Remove any whitespace or newline characters
        token.erase(remove_if(token.begin(), token.end(), ::isspace), token.end());
        if (!token.empty()) {
            return token;
        }
        file.close();
    }
    
    // Fallback to environment variable
    const char* envToken = getenv("TOKEN");
    if (envToken != nullptr) {
        return std::string(envToken);
    }
    
    // Return empty string if no token found - the API client will handle the error
    return "";
}

void marketStreamCallBack(ServiceReply reply)
{
    // Check if this is a MarketDataStream response first
    if (reply.hasMarketDataStreamResponse()) {
        // Use getMarketDataStreamResponse() for MarketDataStream responses
        const auto& streamResponse = reply.getMarketDataStreamResponse();
        std::cout << "[MarketData] Payload type: " << streamResponse.getPayloadTypeString() << std::endl;
        std::cout << streamResponse.debugString() << std::endl;
    } 
    // Check if this is an OrdersStream response
    else if (reply.hasOrdersStreamResponse()) {
        // Use getOrdersStreamResponse() for OrdersStream responses
        const auto& streamResponse = reply.getOrdersStreamResponse();
        std::cout << "[OrdersStream] Payload type: " << streamResponse.getPayloadTypeString() << std::endl;
        std::cout << streamResponse.debugString() << std::endl;
    }
    else if (reply.ptr()) {
        // Use ptr() for non-streaming (unary) responses
        std::cout << reply.ptr()->DebugString() << std::endl;
    } else {
        std::cout << "Empty response" << std::endl;
    }
}

int main()
{
    InvestApiClient client("invest-public-api.tinkoff.ru:443", loadToken());

    //get references to MarketDataStream and OrdersStream services
    auto marketdata = std::dynamic_pointer_cast<MarketDataStream>(client.service("marketdatastream"));
    auto orders = std::dynamic_pointer_cast<OrdersStream>(client.service("ordersstream"));

    //subscribe to Sberbank and Tinkoff prices
    marketdata->SubscribeLastPriceAsync({"BBG004S68104", "BBG00JXPFBN0"}, marketStreamCallBack);

    //subscribe to Bashneft (BANE) and Moscow Exchange (MOEX) shares transactions
    marketdata->SubscribeTradesAsync({"BBG004S68758", "BBG004730JJ5"}, marketStreamCallBack);

    //subscribe to your transactions
    orders->TradesStreamAsync({""}, marketStreamCallBack);

    return 0;
}
