#include "marketdataservice.h"
#include <memory.h>
#include <thread>

using grpc::ClientReaderWriter;

MarketData::MarketData(std::shared_ptr<grpc::Channel> channel, const std::string &token) :
    CustomService(token),
    m_marketDataService(MarketDataService::NewStub(channel))
{

}

MarketData::~MarketData()
{

}

ServiceReply MarketData::GetCandles(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos, CandleInterval interval)
{
    GetCandlesRequest request;
    request.set_instrument_id(instrumentId);
    auto from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    auto  to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    request.set_interval(interval);
    GetCandlesResponse reply;
    Status status = m_marketDataService->GetCandles(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetCandlesResponse>(status, reply);
}

ServiceReply MarketData::GetCandlesFigiOld(const std::string &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos, CandleInterval interval)
{
    GetCandlesRequest request;
    request.set_figi(figi);
    auto from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    auto  to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    request.set_interval(interval);
    GetCandlesResponse reply;
    Status status = m_marketDataService->GetCandles(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetCandlesResponse>(status, reply);
}

ServiceReply MarketData::GetLastPrices(const Strings &instrumentIds)
{
    GetLastPricesRequest request;
    for (auto &id: instrumentIds) request.add_instrument_id(id);
    GetLastPricesResponse reply;
    Status status = m_marketDataService->GetLastPrices(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetLastPricesResponse>(status, reply);
}

ServiceReply MarketData::GetOrderBook(const std::string &instrumentId, int32_t depth)
{
    GetOrderBookRequest request;
    request.set_instrument_id(instrumentId);
    request.set_depth(depth);
    GetOrderBookResponse reply;
    Status status = m_marketDataService->GetOrderBook(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetOrderBookResponse>(status, reply);
}

ServiceReply MarketData::GetTradingStatus(const std::string &instrumentId)
{
    GetTradingStatusRequest request;
    request.set_instrument_id(instrumentId);
    GetTradingStatusResponse reply;
    Status status = m_marketDataService->GetTradingStatus(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetTradingStatusResponse>(status, reply);
}

ServiceReply MarketData::GetTradingStatusFigiOld(const std::string &figi)
{
    GetTradingStatusRequest request;
    request.set_figi(figi);
    GetTradingStatusResponse reply;
    Status status = m_marketDataService->GetTradingStatus(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetTradingStatusResponse>(status, reply);
}

ServiceReply MarketData::GetLastTrades(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    GetLastTradesRequest request;
    request.set_instrument_id(instrumentId);
    auto from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    auto  to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    GetLastTradesResponse reply;
    Status status = m_marketDataService->GetLastTrades(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetLastTradesResponse>(status, reply);
}

ServiceReply MarketData::GetTradingStatuses(const std::vector<std::string> &instrumentIds)
{
    GetTradingStatusesRequest request;
    for (const auto& id : instrumentIds) {
        request.add_instrument_id(id);
    }
    GetTradingStatusesResponse reply;
    Status status = m_marketDataService->GetTradingStatuses(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetTradingStatusesResponse>(status, reply);
}

ServiceReply MarketData::GetClosePrices(const std::vector<std::string> &instrumentIds)
{
    GetClosePricesRequest request;
    for (const auto& id : instrumentIds) {
        auto instrument = request.add_instruments();
        instrument->set_instrument_id(id);
    }
    GetClosePricesResponse reply;
    Status status = m_marketDataService->GetClosePrices(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetClosePricesResponse>(status, reply);
}
