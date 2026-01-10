#ifndef MARKETDATASERVICE_H
#define MARKETDATASERVICE_H

#include <vector>
#include <grpcpp/grpcpp.h>
#include "customservice.h"
#include "marketdata.grpc.pb.h"
#include "commontypes.h"


using grpc::Channel;
using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Сервис получения биржевой информации

    Сервис получения биржевой информации:
    1. свечи;
    2. стаканы;
    3. торговые статусы;
    4. лента сделок;
    5. цены закрытия.
*/
class TINKOFFINVESTSDK_EXPORT MarketData: public CustomService
{

public:  
    MarketData(std::shared_ptr<Channel> channel, const std::string &token);
    ~MarketData();

    /// Метод запроса исторических свечей по инструменту (использует instrument_id)
    ServiceReply GetCandles(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos, CandleInterval interval);
    /// Метод запроса исторических свечей по инструменту (устаревший метод, использующий figi)
    ServiceReply GetCandlesFigiOld(const std::string &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos, CandleInterval interval);
    /// Метод запроса последних цен по инструментам (использует instrument_id)
    ServiceReply GetLastPrices(const std::vector<std::string> &instrumentIds);
    /// Метод получения стакана по инструменту (использует instrument_id)
    ServiceReply GetOrderBook(const std::string &instrumentId, int32_t depth);
    /// Метод запроса статуса торгов по инструменту (использует instrument_id)
    ServiceReply GetTradingStatus(const std::string &instrumentId);
    /// Метод запроса статуса торгов по инструменту (устаревший метод, использующий figi)
    ServiceReply GetTradingStatusFigiOld(const std::string &figi);
    /// Метод запроса статусов торгов по инструментам (batch) (использует instrument_id)
    ServiceReply GetTradingStatuses(const std::vector<std::string> &instrumentIds);
    /// Метод запроса последних обезличенных сделок по инструменту (использует instrument_id)
    ServiceReply GetLastTrades(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод запроса цен закрытия торговой сессии по инструментам
    ServiceReply GetClosePrices(const std::vector<std::string> &instrumentIds);

private:
    std::unique_ptr<MarketDataService::Stub> m_marketDataService;

};

#endif // MARKETDATASERVICE_H
