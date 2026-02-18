#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"
#include <iostream>

void marketStreamCallBack(ServiceReply reply)
{
    // Check if this is a streaming response (MarketDataStreamResponse)
    if (reply.hasMarketDataStreamResponse()) {
        // Use getMarketDataStreamResponse() for streaming responses
        const auto& streamResponse = reply.getMarketDataStreamResponse();
        std::cout << "Payload type: " << streamResponse.getPayloadTypeString() << std::endl;
        std::cout << streamResponse.debugString() << std::endl;
    } else if (reply.ptr()) {
        // Use ptr() for non-streaming (unary) responses
        std::cout << reply.ptr()->DebugString() << std::endl;
    } else {
        std::cout << "Empty response" << std::endl;
    }
}

int main()
{
    InvestApiClient client("invest-public-api.tinkoff.ru:443", getenv("TOKEN"));

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
