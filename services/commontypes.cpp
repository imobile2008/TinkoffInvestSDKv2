#include "sandbox.grpc.pb.h"
#include "commontypes.h"
#include "marketdatastreamresponse.h"
#include "ordersstreamresponse.h"

using namespace tinkoff::public_::invest::api::contract::v1;

ServiceReply::ServiceReply()
    : m_replyPtr(nullptr), m_status(), m_errorMessage(""), m_streamResponse(nullptr), m_ordersStreamResponse(nullptr)
{

}

ServiceReply::ServiceReply(const std::shared_ptr<google::protobuf::Message>  protoMsg, const Status& status, const std::string& messageIfError) 
    : m_replyPtr(protoMsg), m_status(status), m_errorMessage(messageIfError), m_streamResponse(nullptr), m_ordersStreamResponse(nullptr)
{

}

ServiceReply::ServiceReply(const MarketDataStreamResponse& streamResponse, const Status& status)
    : m_replyPtr(nullptr), 
      m_status(status), 
      m_errorMessage(""), 
      m_streamResponse(std::make_shared<MarketDataStreamResponse>(streamResponse)),
      m_ordersStreamResponse(nullptr)
{
    // Ensure proper initialization - throw if allocation failed
    if (!m_streamResponse) {
        throw std::runtime_error("Failed to create MarketDataStreamResponse");
    }
}

ServiceReply::ServiceReply(const OrdersStreamResponse& streamResponse, const Status& status)
    : m_replyPtr(nullptr), 
      m_status(status), 
      m_errorMessage(""), 
      m_streamResponse(nullptr),
      m_ordersStreamResponse(std::make_shared<OrdersStreamResponse>(streamResponse))
{
    // Ensure proper initialization - throw if allocation failed
    if (!m_ordersStreamResponse) {
        throw std::runtime_error("Failed to create OrdersStreamResponse");
    }
}

const std::string ServiceReply::accountID(const int i)
{
    auto response = dynamic_cast<GetAccountsResponse *>(ptr().get());
    if (response && i < response->accounts_size())
    {
        return response->accounts(i).id();
    } else {
        return "";
    }
}

const std::string ServiceReply::accountName(const int i)
{
    auto response = dynamic_cast<GetAccountsResponse *>(ptr().get());
    if (response && i < response->accounts_size())
    {
        return response->accounts(i).name();
    } else {
        return "";
    }
}

int ServiceReply::accountCount()
{
    auto response = dynamic_cast<GetAccountsResponse *>(ptr().get());
    if (response)
    {
        return response->accounts_size();
    } else {
        return 0;
    }
}

const std::shared_ptr<google::protobuf::Message> ServiceReply::ptr()
{
    return m_replyPtr;
}

const Status& ServiceReply::GetStatus() const { return m_status; }

const std::string& ServiceReply::GetErrorMessage() const { return m_errorMessage; }

bool ServiceReply::hasMarketDataStreamResponse() const
{
    return m_streamResponse != nullptr;
}

const MarketDataStreamResponse& ServiceReply::getMarketDataStreamResponse() const
{
    if (!m_streamResponse) {
        throw std::runtime_error("ServiceReply does not contain a MarketDataStreamResponse");
    }
    return *m_streamResponse;
}

bool ServiceReply::hasOrdersStreamResponse() const
{
    return m_ordersStreamResponse != nullptr;
}

const OrdersStreamResponse& ServiceReply::getOrdersStreamResponse() const
{
    if (!m_ordersStreamResponse) {
        throw std::runtime_error("ServiceReply does not contain an OrdersStreamResponse");
    }
    return *m_ordersStreamResponse;
}
