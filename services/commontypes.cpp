#include "sandbox.grpc.pb.h"
#include "commontypes.h"
#include "marketdatastreamresponse.h"

using namespace tinkoff::public_::invest::api::contract::v1;

ServiceReply::ServiceReply()
    : m_replyPtr(nullptr), m_status(), m_errorMessage(""), m_streamResponse(nullptr)
{

}

ServiceReply::ServiceReply(const std::shared_ptr<google::protobuf::Message>  protoMsg, const Status& status, const std::string& messageIfError) 
    : m_replyPtr(protoMsg), m_status(status), m_errorMessage(messageIfError), m_streamResponse(nullptr)
{

}

ServiceReply::ServiceReply(const MarketDataStreamResponse& streamResponse, const Status& status)
    : m_replyPtr(nullptr), m_status(status), m_errorMessage(""), m_streamResponse(std::make_shared<MarketDataStreamResponse>(streamResponse))
{

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
