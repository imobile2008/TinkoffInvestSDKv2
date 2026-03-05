#ifndef ORDERSSERVICE_H
#define ORDERSSERVICE_H

#include "customservice.h"
#include <grpcpp/grpcpp.h>
#include "orders.grpc.pb.h"
#include "commontypes.h"

using grpc::Channel;
using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief  Сервис торговых поручений

    Сервис предназначен для работы с торговыми поручениями:
    1.выставление;
    2.отмена;
    3.изменение;
    4.получение статуса;
    5.расчёт полной стоимости;
    6.получение списка заявок.
*/
class TINKOFFINVESTSDK_EXPORT Orders: public CustomService
{

public:  
    Orders(std::shared_ptr<Channel> channel, const std::string &token);
    ~Orders();

    /// Метод выставления заявки (использует instrument_id)
    ServiceReply PostOrder(const std::string &instrumentId, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId);
    /// Метод выставления заявки (устаревший метод, использующий figi)
    ServiceReply PostOrderFigiOld(const std::string &figi, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId);
    /// Метод отмены биржевой заявки
    ServiceReply CancelOrder(const std::string &accountId, const std::string &orderId);
    /// Метод изменения заявки
    ServiceReply ReplaceOrder(const std::string &accountId, const std::string &orderId, int64_t quantity, int64_t units, int32_t nano, PriceType priceType, const std::string &idempotencyKey);
    /// Метод получения статуса торгового поручения
    ServiceReply GetOrderState(const std::string &accountId, const std::string &orderId);
    /// Метод получения списка активных заявок по счёту
    ServiceReply GetOrders(const std::string &accountId);
    /// Метод выставления асинхронной заявки
    ServiceReply PostOrderAsync(const std::string &instrumentId, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId, const std::string &priceSalt);
    /// Метод получения максимального количества лотов
    ServiceReply GetMaxLots(const std::string &instrumentId, const std::string &accountId);
    /// Метод получения цены заявки
    ServiceReply GetOrderPrice(const std::string &instrumentId, int64_t quantity, OrderDirection direction, const std::string &accountId, OrderType orderType);

private:
    std::unique_ptr<OrdersService::Stub> m_ordersService;

};

#endif // ORDERSSERVICE_H
