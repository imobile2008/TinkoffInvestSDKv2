#ifndef SANDBOXSERVICE_H
#define SANDBOXSERVICE_H

#include "customservice.h"
#include <grpcpp/grpcpp.h>
#include "sandbox.grpc.pb.h"
#include "stoporders.grpc.pb.h"
#include "commontypes.h"

using grpc::Channel;
using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief  Сервис для работы с песочницей

    Сервис для работы с песочницей TINKOFF INVEST API
*/
class TINKOFFINVESTSDK_EXPORT Sandbox: public CustomService
{

public:  
    Sandbox(std::shared_ptr<Channel> channel, const std::string &token);
    ~Sandbox();

    /// Регистрация счёта в песочнице
    ServiceReply OpenSandboxAccount();
    /// Получение счетов в песочнице
    ServiceReply GetSandboxAccounts();
    /// Закрытие счёта в песочнице
    ServiceReply CloseSandboxAccount(const std::string &accountId);
    /// Выставление торгового поручения в песочнице (использует instrument_id)
    ServiceReply PostSandboxOrder(const std::string &instrumentId, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId);
    /// Выставление торгового поручения в песочнице (устаревший метод, использующий figi)
    ServiceReply PostSandboxOrderFigiOld(const std::string &figi, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId);
    /// Изменение выставленной заявки в песочнице
    ServiceReply ReplaceSandboxOrder(const std::string &accountId, const std::string &orderId, int64_t quantity, int64_t units, int32_t nano, PriceType priceType, const std::string &idempotencyKey);
    /// Получение списка активных заявок по счёту в песочнице
    ServiceReply GetSandboxOrders(const std::string &accountId);
    /// Отмена торгового поручения в песочнице
    ServiceReply CancelSandboxOrder(const std::string &accountId, const std::string  &orderId);
    /// Получение статуса заявки в песочнице
    ServiceReply GetSandboxOrderState(const std::string  &accountId, const std::string  &orderId);
    /// Получение позиций по виртуальному счёту песочницы
    ServiceReply GetSandboxPositions(const std::string  &accountId);
    /// Получение операций в песочнице по номеру счёта
    ServiceReply GetSandboxOperations(const std::string  &accountId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos);
    /// Получение операций в песочнице по номеру счёта с пагинацией
    ServiceReply GetSandboxOperationsByCursor(const std::string &accountId, const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, 
                                              int64_t toseconds, int32_t tonanos, const std::string &cursor, int32_t limit, 
                                              const std::vector<OperationType> &operationTypes, OperationState state,
                                              bool withoutCommissions, bool withoutTrades, bool withoutOvernights);
    /// Получение портфолио в песочнице
    ServiceReply GetSandboxPortfolio(const std::string  &accountId, PortfolioRequest_CurrencyRequest currency);
    /// Пополнение счёта в песочнице
    ServiceReply SandboxPayIn(const std::string &accountId, const std::string  &currency, int64_t units, int32_t nano);
    /// Получение доступного остатка для вывода средств в песочнице
    ServiceReply GetSandboxWithdrawLimits(const std::string &accountId);
    /// Выставление асинхронного торгового поручения в песочнице
    ServiceReply PostSandboxOrderAsync(const std::string &instrumentId, int64_t quantity, int64_t units, int32_t nano, OrderDirection direction, const std::string &accountId, OrderType orderType, const std::string &orderId, const std::string &priceSalt);
    /// Получение цены заявки в песочнице
    ServiceReply GetSandboxOrderPrice(const std::string &instrumentId, int64_t quantity, OrderDirection direction, const std::string &accountId, OrderType orderType);
    /// Получение максимального количества лотов в песочнице
    ServiceReply GetSandboxMaxLots(const std::string &instrumentId, const std::string &accountId);
    /// Выставление стоп-заявки в песочнице
    ServiceReply PostSandboxStopOrder(const std::string &instrumentId, int64_t quantity, int64_t units, int32_t nano, int64_t stopunits, int32_t stopnano, StopOrderDirection direction, const std::string &accountId, StopOrderExpirationType expirationType, StopOrderType stopOrderType, int64_t expireSeconds, int32_t expireNanos);
    /// Получение списка стоп-заявок в песочнице
    ServiceReply GetSandboxStopOrders(const std::string &accountId);
    /// Отмена стоп-заявки в песочнице
    ServiceReply CancelSandboxStopOrder(const std::string &accountId, const std::string &stopOrderId);

private:
    std::unique_ptr<SandboxService::Stub> m_sandboxService;

};

#endif // SANDBOXSERVICE_H
