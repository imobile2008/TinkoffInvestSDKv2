#include "usersservice.h"

Users::Users(std::shared_ptr<grpc::Channel> channel, const std::string &token) :
    CustomService(token),
    m_usersService(UsersService::NewStub(channel))
{

}

Users::~Users()
{

}

ServiceReply Users::GetAccounts()
{
    GetAccountsRequest request;
    GetAccountsResponse reply;
    Status status = m_usersService->GetAccounts(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetAccountsResponse>(status, reply);
}

ServiceReply Users::GetMarginAttributes(const std::string &accountId)
{
    GetMarginAttributesRequest request;
    request.set_account_id(accountId);
    GetMarginAttributesResponse reply;
    Status status = m_usersService->GetMarginAttributes(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetMarginAttributesResponse>(status, reply);
}

ServiceReply Users::GetUserTariff()
{
    GetUserTariffRequest request;
    GetUserTariffResponse reply;
    Status status = m_usersService->GetUserTariff(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetUserTariffResponse>(status, reply);
}

ServiceReply Users::GetInfo()
{
    GetInfoRequest request;
    GetInfoResponse reply;
    Status status = m_usersService->GetInfo(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetInfoResponse>(status, reply);
}

ServiceReply Users::GetBankAccounts()
{
    // GetBankAccounts method not available in current API
    // GetBankAccountsRequest request;
    // GetBankAccountsResponse reply;
    // Status status = m_usersService->GetBankAccounts(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<GetBankAccountsResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetBankAccounts method not available in current API version");
}

ServiceReply Users::CurrencyTransfer(const std::string &accountId, const std::string &fromCurrency, const std::string &toCurrency, int64_t units, int32_t nano)
{
    // CurrencyTransfer method not available in current API
    // CurrencyTransferRequest request;
    // request.set_from_account_id(accountId);
    // // Set the currency in the amount object
    // auto amount = new MoneyValue();
    // amount->set_units(units);
    // amount->set_nano(nano);
    // amount->set_currency(toCurrency);  // Use toCurrency as the transfer currency
    // request.set_allocated_amount(amount);
    // CurrencyTransferResponse reply;
    // Status status = m_usersService->CurrencyTransfer(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<CurrencyTransferResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "CurrencyTransfer method not available in current API version");
}

ServiceReply Users::PayIn(const std::string &accountId, const std::string &currency, int64_t units, int32_t nano)
{
    // PayIn method not available in current API
    // PayInRequest request;
    // request.set_to_account_id(accountId);
    // auto amount = new MoneyValue();
    // amount->set_units(units);
    // amount->set_nano(nano);
    // amount->set_currency(currency);
    // request.set_allocated_amount(amount);
    // PayInResponse reply;
    // Status status = m_usersService->PayIn(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<PayInResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "PayIn method not available in current API version");
}

ServiceReply Users::GetAccountValues(const std::string &accountId)
{
    // GetAccountValues method not available in current API
    // GetAccountValuesRequest request;
    // request.add_accounts(accountId);
    // GetAccountValuesResponse reply;
    // Status status = m_usersService->GetAccountValues(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<GetAccountValuesResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetAccountValues method not available in current API version");
}
