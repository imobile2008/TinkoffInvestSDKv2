#include "investapiclient.h"
#include "marketdatastreamservice.h"
#include "ordersstreamservice.h"

void marketStreamCallBack(ServiceReply reply)
{
    std::cout << reply.ptr()->DebugString() << std::endl;
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
