#ifndef MARKETDATASTREAMSERVICE_H
#define MARKETDATASTREAMSERVICE_H

#include <vector>
#include <set>
#include <grpcpp/grpcpp.h>
#include "marketdata.grpc.pb.h"
#include "customservice.h"
#include "commontypes.h"

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Сервис получения биржевой информации в режиме стриминга

    Сервис получения биржевой информации:
    1. свечи;
    2. стаканы;
    3. торговые статусы;
    4. лента сделок.
*/
class TINKOFFINVESTSDK_EXPORT MarketDataStream: public CustomService
{

public:
    MarketDataStream(std::shared_ptr<Channel> channel, const std::string &token);
    ~MarketDataStream();

    /// Запрос подписки на свечи, блокирующий вызов (использует instrument_id)
    bool SubscribeCandles(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    /// Запрос подписки на свечи, блокирующий вызов (устаревший метод, использующий figi)
    bool SubscribeCandlesFigiOld(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    /// Запрос подписки на стаканы, блокирующий вызов (использует instrument_id)
    bool SubscribeOrderBook(const Strings &instrumentIds, int32_t depth, CallbackFunc callback);
    /// Запрос подписки на стаканы, блокирующий вызов (устаревший метод, использующий figi)
    bool SubscribeOrderBookFigiOld(const std::string &figi, int32_t depth, CallbackFunc callback);
    /// Запрос подписки на ленту обезличенных сделок, блокирующий вызов (использует instrument_id)
    bool SubscribeTrades(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на ленту обезличенных сделок, блокирующий вызов (устаревший метод, использующий figi)
    bool SubscribeTradesFigiOld(const Strings &figis, CallbackFunc callback);
    /// Запрос подписки на торговые статусы инструментов, блокирующий вызов (использует instrument_id)
    bool SubscribeInfo(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на торговые статусы инструментов, блокирующий вызов (устаревший метод, использующий figi)
    bool SubscribeInfoFigiOld(const Strings &figis, CallbackFunc callback);
    /// Запрос подписки на последние цены, блокирующий вызов (использует instrument_id)
    bool SubscribeLastPrice(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на последние цены, блокирующий вызов (устаревший метод, использующий figi)
    bool SubscribeLastPriceFigiOld(const Strings &figis, CallbackFunc callback);

    /// Отмена подписки на свечи, блокирующий вызов
    bool UnSubscribeCandles();
    /// Отмена подписки на стаканы, блокирующий вызов
    bool UnSubscribeOrderBook();
    /// Отмена подписки на ленту обезличенных сделок, блокирующий вызов
    bool UnSubscribeTrades();
    /// Отмена подписки на последние цены, блокирующий вызов
    bool UnSubscribeLastPrice();
    /// Отмена подписки на торговые статусы инструментов, блокирующий вызов
    bool UnSubscribeInfo();

    /// Запрос подписки на свечи, асинхронный вызов (использует instrument_id)
    void SubscribeCandlesAsync(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    /// Запрос подписки на свечи, асинхронный вызов (устаревший метод, использующий figi)
    void SubscribeCandlesFigiOldAsync(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    /// Запрос подписки на стаканы, асинхронный вызов (использует instrument_id)
    void SubscribeOrderBookAsync(const Strings &instrumentIds, int32_t depth, CallbackFunc callback);
    /// Запрос подписки на стаканы, асинхронный вызов (устаревший метод, использующий figi)
    void SubscribeOrderBookFigiOldAsync(const Strings &figis, int32_t depth, CallbackFunc callback);
    /// Запрос подписки на ленту обезличенных сделок, асинхронный вызов (использует instrument_id)
    void SubscribeTradesAsync(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на ленту обезличенных сделок, асинхронный вызов (устаревший метод, использующий figi)
    void SubscribeTradesFigiOldAsync(const Strings &figis, CallbackFunc callback);
    /// Запрос подписки на торговые статусы инструментов, асинхронный вызов (использует instrument_id)
    void SubscribeInfoAsync(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на торговые статусы инструментов, асинхронный вызов (устаревший метод, использующий figi)
    void SubscribeInfoFigiOldAsync(const Strings &figis, CallbackFunc callback);
    /// Запрос подписки на последние цены, асинхронный вызов (использует instrument_id)
    void SubscribeLastPriceAsync(const Strings &instrumentIds, CallbackFunc callback);
    /// Запрос подписки на последние цены, асинхронный вызов (устаревший метод, использующий figi)
    void SubscribeLastPriceFigiOldAsync(const Strings &figis, CallbackFunc callback);

    /// Отмена подписки на свечи, асинхронный вызов
    void UnSubscribeCandlesAsync();
    /// Отмена подписки на стаканы, асинхронный вызов
    void UnSubscribeOrderBookAsync();
    /// Отмена подписки на ленту обезличенных сделок, асинхронный вызов
    void UnSubscribeTradesAsync();
    /// Отмена подписки на последние цены, асинхронный вызов
    void UnSubscribeLastPriceAsync();
    /// Отмена подписки на торговые статусы инструментов, асинхронный вызов
    void UnSubscribeInfoAsync();

private:
    std::unique_ptr<MarketDataStreamService::Stub> m_marketDataStreamService;
    std::set<std::shared_ptr<MarketDataHandler>> m_currentHandlers;
    void SendRequest(const MarketDataRequest &request, CallbackFunc callback = nullptr);

};

#endif // MARKETDATASTREAMSERVICE_H
