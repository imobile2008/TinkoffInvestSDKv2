#include <thread>
#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "marketdatastreamresponse.h"

void marketStreamCallBack(ServiceReply reply)
{
    // Check if this is a streaming response with payload type detection
    if (reply.hasMarketDataStreamResponse()) {
        const auto& streamResponse = reply.getMarketDataStreamResponse();
        
        // Log the payload type for debugging
        std::cout << "Received payload type: " << streamResponse.getPayloadTypeString() << std::endl;
        
        // Handle different payload types properly
        switch (streamResponse.getPayloadType()) {
            case MarketDataStreamResponse::PayloadType::TRADING_STATUS:
                {
                    const auto& tradingStatus = streamResponse.getTradingStatus();
                    std::cout << "Trading Status for " << tradingStatus.figi() 
                              << ": " << tradingStatus.trading_status() << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::SUBSCRIBE_INFO_RESPONSE:
                {
                    // This confirms subscription was successful
                    std::cout << "Subscription confirmed. Tracking ID: " 
                              << streamResponse.getTrackingId() << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::CANDLE:
                {
                    const auto& candle = streamResponse.getCandle();
                    std::cout << "Candle received (use debugString for details)" << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::ORDERBOOK:
                {
                    const auto& orderbook = streamResponse.getOrderBook();
                    std::cout << "OrderBook depth: " << orderbook.depth() << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::TRADE:
                {
                    // Trade received - use debugString() for full details
                    std::cout << "Trade received" << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::LAST_PRICE:
                {
                    const auto& lastPrice = streamResponse.getLastPrice();
                    std::cout << "Last Price received" << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::PING:
                {
                    std::cout << "Keep-alive ping received" << std::endl;
                }
                break;
                
            case MarketDataStreamResponse::PayloadType::UNKNOWN:
            default:
                {
                    std::cout << "Unknown payload type received" << std::endl;
                }
                break;
        }
    } else {
        // Fallback to original behavior for non-streaming responses
        std::cout << reply.ptr()->DebugString() << std::endl;
    }
}

// Alternative callback using MarketDataStreamResponse directly
void marketStreamCallBackAdvanced(MarketDataStreamResponse response)
{
    std::cout << "Advanced callback - Payload: " << response.getPayloadTypeString() << std::endl;
    
    // Type-safe access without ServiceReply wrapper
    if (response.isPayloadType(MarketDataStreamResponse::PayloadType::TRADING_STATUS)) {
        const auto& tradingStatus = response.getTradingStatus();
        std::cout << "Trading Status: " << tradingStatus.figi() << std::endl;
    }
}

int main()
{    
    InvestApiClient client("invest-public-api.tinkoff.ru:443", getenv("TOKEN"));

    //get reference to MarketDataStream service
    auto marketdata = std::dynamic_pointer_cast<MarketDataStream>(client.service("marketdatastream"));

    //subscribe to NVIDIA and Tesla Motors prices and start streaming
    std::thread th1(
                [marketdata](){marketdata->SubscribeLastPrice({"BBG000BBJQV0", "BBG000N9MNX3"}, marketStreamCallBack);}
    );

    //subscribe to Bashneft (BANE) and Moscow Exchange (MOEX) shares transactions
    std::thread th2(
                [marketdata](){marketdata->SubscribeTradesAsync({"BBG004S68758", "BBG004730JJ5"}, marketStreamCallBack);}
    );

    th1.join();
    th2.join();

    return 0;
}
