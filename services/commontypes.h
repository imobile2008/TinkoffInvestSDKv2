#ifndef COMMONTYPES_H
#define COMMONTYPES_H

#include <grpcpp/grpcpp.h>
#include <functional>
#include "google/protobuf/message.h"
#include "tinkoffinvestsdk_export.h"

#include "marketdatastreamresponse.h"
#include "ordersstreamresponse.h"
#include "operationsstreamresponse.h"

using grpc::Status;

static const std::string APP_NAME = "imobile2008.TinkoffInvestSDKv2";

/*!
    \brief Класс-обертка над proto-ответами сервисов

    Данный класс используется при передачи информации от сервисов клиентам
*/
class TINKOFFINVESTSDK_EXPORT ServiceReply
{

public:
    ServiceReply();
    ServiceReply(const std::shared_ptr<google::protobuf::Message> protoMsg, const Status& status, const std::string& messageIfError = "");
    
    // Constructor for MarketDataStreamResponse compatibility
    ServiceReply(const MarketDataStreamResponse& streamResponse, const Status& status = Status());
    
    // Constructor for OrdersStreamResponse compatibility
    ServiceReply(const OrdersStreamResponse& streamResponse, const Status& status = Status());
    
    // Constructor for OperationsStreamResponse compatibility
    ServiceReply(const OperationsStreamResponse& streamResponse, const Status& status = Status());
    
    // Constructor for error status with custom message
    ServiceReply(const Status& status, const std::string& messageIfError);
    
    const std::shared_ptr<google::protobuf::Message> ptr();
    const std::string accountID(const int i);
    const std::string accountName(const int i);
    int accountCount();
    const Status& GetStatus() const;
	const std::string& GetErrorMessage() const;

    // MarketDataStreamResponse access methods
    bool hasMarketDataStreamResponse() const;
    const MarketDataStreamResponse& getMarketDataStreamResponse() const;

    // OrdersStreamResponse access methods
    bool hasOrdersStreamResponse() const;
    const OrdersStreamResponse& getOrdersStreamResponse() const;

    // OperationsStreamResponse access methods
    bool hasOperationsStreamResponse() const;
    const OperationsStreamResponse& getOperationsStreamResponse() const;

	template<class T>
    static const ServiceReply prepareServiceAnswer(const Status &status, const T &protoMsg, const std::string& messageIfError = "")
    {
        std::string errorMsg = messageIfError;
        // If no custom error message provided, use gRPC error message if status is not OK
        if (errorMsg.empty() && !status.ok()) {
            errorMsg = status.error_message();
        }
        return (status.ok()) ? ServiceReply(std::make_shared<T>(protoMsg), status) : ServiceReply(nullptr, status, errorMsg);
    }

private:
    std::shared_ptr<google::protobuf::Message> m_replyPtr;
    Status m_status;
	std::string m_errorMessage;
    
    // Optional MarketDataStreamResponse for streaming support
    std::shared_ptr<MarketDataStreamResponse> m_streamResponse;
    
    // Optional OrdersStreamResponse for orders streaming support
    std::shared_ptr<OrdersStreamResponse> m_ordersStreamResponse;
    
    // Optional OperationsStreamResponse for operations streaming support
    std::shared_ptr<OperationsStreamResponse> m_operationsStreamResponse;
};

using CallbackFunc = std::function<void (ServiceReply)>;

using Strings = std::vector<std::string>;

//////////////////////////////////////////////////////////////////////////
// Enums from common.proto
//////////////////////////////////////////////////////////////////////////


#endif // COMMONTYPES_H

