#ifndef INSTRUMENTSSERVICE_H
#define INSTRUMENTSSERVICE_H

#include "customservice.h"
#include <grpcpp/grpcpp.h>
#include "instruments.grpc.pb.h"
#include "commontypes.h"
#include <memory>

using grpc::Channel;
using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief  Сервис для работы с различными инструментами

    Сервис предназначен для получения:
    1.информации об инструментах;
    2 расписания торговых сессий;
    3.календаря выплат купонов по облигациям;
    4.размера гарантийного обеспечения по фьючерсам;
    5.дивидендов по ценной бумаге.
*/
class TINKOFFINVESTSDK_EXPORT Instruments: public CustomService
{

public:  
    Instruments(std::shared_ptr<Channel> channel, const std::string &token);
    ~Instruments();

    /// Метод получения расписания торгов торговых площадок
    ServiceReply TradingSchedules(const std::string &exchange, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения облигации по её идентификатору
    ServiceReply BondBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка облигаций
    ServiceReply Bonds(InstrumentStatus instrumentStatus);
    /// Метод получения графика выплат купонов по облигации
    ServiceReply GetBondCoupons(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения валюты по её идентификатору
    ServiceReply CurrencyBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка валют
    ServiceReply Currencies(InstrumentStatus instrumentStatus);
    /// Метод получения инвестиционного фонда по его идентификатору
    ServiceReply EtfBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка инвестиционных фондов
    ServiceReply Etfs(InstrumentStatus instrumentStatus);
    /// Метод получения фьючерса по его идентификатору
    ServiceReply FutureBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка фьючерсов
    ServiceReply Futures(InstrumentStatus instrumentStatus);
    /// Метод получения акции по её идентификатору
    ServiceReply ShareBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка акций
    ServiceReply Shares(InstrumentStatus instrumentStatus);
    /// Метод получения накопленного купонного дохода по облигации
    ServiceReply GetAccruedInterests(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения размера гарантийного обеспечения по фьючерсам
    ServiceReply GetFuturesMargin(const std::string  &figi);
    /// Метод получения основной информации об инструменте
    ServiceReply GetInstrumentBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод для получения событий выплаты дивидендов по инструменту
    ServiceReply GetDividends(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения актива по его идентификатору
    ServiceReply GetAssetBy(const std::string  &id);
    /// Метод получения списка активов
    ServiceReply GetAssets();
    /// Метод получения избранных инструментов
    ServiceReply GetFavorites();
    /// Метод редактирования избранных инструментов
    ServiceReply EditFavorites(const std::vector<EditFavoritesRequestInstrument> &instruments, EditFavoritesActionType actionType);
    /// Метод получения событий по облигации
    ServiceReply GetBondEvents(const std::string &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения опциона по его идентификатору
    ServiceReply OptionBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка опционов
    ServiceReply Options(InstrumentStatus instrumentStatus);
    /// Метод получения списка опционов по фильтру
    ServiceReply OptionsBy(InstrumentStatus instrumentStatus, const std::string &underlyingFigi, std::string &isoCurrency);
    /// Метод получения фонда доверительного управления (ДУ) по его идентификатору
    ServiceReply DfaBy(InstrumentIdType idType, const std::string &classCode, const std::string &id);
    /// Метод получения списка фондов доверительного управления (ДУ)
    ServiceReply Dfas(InstrumentStatus instrumentStatus);
    /// Метод получения индикативных данных по инструменту
    /// Not available in current API version
    // ServiceReply Indicatives(const std::string &instrumentId);
    /// Метод создания группы избранного
    /// Not available in current API version
    // ServiceReply CreateFavoriteGroup(const std::string &name, const std::vector<std::string> &instrumentIds);
    /// Метод удаления группы избранного
    /// Not available in current API version
    // ServiceReply DeleteFavoriteGroup(const std::string &favoriteId);
    /// Метод получения списка групп избранного
    ServiceReply GetFavoriteGroups();
    /// Метод получения списка стран
    ServiceReply GetCountries();
    /// Метод поиска инструмента по идентификатору
    ServiceReply FindInstrument(const std::string &query);
    /// Метод получения списка брендов
    /// Not available in current API version
    // ServiceReply GetBrands(const std::string &instrumentId);
    /// Метод получения бренда по идентификатору
    ServiceReply GetBrandBy(const std::string &brandId);
    /// Метод получения фундаментальных данных по активу
    /// Not available in current API version
    // ServiceReply GetAssetFundamentals(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Метод получения отчётов по активу
    /// Not available in current API version
    // ServiceReply GetAssetReports(const std::string &assetId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    // GetConsensusForecasts removed - does not exist in current API
    // /// Метод получения консенсус-прогнозов
    // ServiceReply GetConsensusForecasts(const std::string &instrumentId);
    /// Метод получения прогноза по инструменту
    ServiceReply GetForecastBy(const std::string &instrumentId);
    /// Метод получения процентных ставок
    ServiceReply GetRiskRates(const std::string &instrumentId);

private:
    std::unique_ptr<InstrumentsService::Stub> m_instrumentsService;

};

#endif // INSTRUMENTSSERVICE_H
