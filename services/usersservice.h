#ifndef USERSSERVICE_H
#define USERSSERVICE_H

#include "customservice.h"
#include <grpcpp/grpcpp.h>
#include "users.grpc.pb.h"
#include "commontypes.h"

using grpc::Channel;

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief  Сервис работы со счетами пользователя

    Сервис предназначен для получения:
    1.списка счетов пользователя;
    2.маржинальных показателе по счёту.
*/
class TINKOFFINVESTSDK_EXPORT Users: public CustomService
{

public:  
    Users(std::shared_ptr<Channel> channel, const std::string &token);
    ~Users();

    /// Метод получения счетов пользователя
    ServiceReply GetAccounts();
    /// Расчёт маржинальных показателей по счёту
    ServiceReply GetMarginAttributes(const std::string &accountId);
    /// Запрос тарифа пользователя
    ServiceReply GetUserTariff();
    /// Метод получения информации о пользователе
    ServiceReply GetInfo();
    /// Метод получения банковских счетов
    ServiceReply GetBankAccounts();
    /// Метод перевода валюты между счетами
    ServiceReply CurrencyTransfer(const std::string &accountId, const std::string &fromCurrency, const std::string &toCurrency, int64_t units, int32_t nano);
    /// Метод пополнения брокерского счёта
    ServiceReply PayIn(const std::string &accountId, const std::string &currency, int64_t units, int32_t nano);
    /// Метод получения значений счёта
    ServiceReply GetAccountValues(const std::string &accountId);

private:
    std::unique_ptr<UsersService::Stub> m_usersService;
};

#endif // USERSSERVICE_H
