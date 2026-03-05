#ifndef OPERATIONSSTREAMSERVICE_H
#define OPERATIONSSTREAMSERVICE_H

#include <memory>
#include <functional>
#include <atomic>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "operations.grpc.pb.h"
#include "customservice.h"
#include "commontypes.h"
#include "operationsstreamresponse.h"

using grpc::Channel;
using grpc::ClientReader;

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Stream state tracking for OperationsStream
*/
enum OperationsStreamState {
    kOperationsStreamDisconnected = 0,
    kOperationsStreamConnecting = 1,
    kOperationsStreamConnected = 2,
    kOperationsStreamStreaming = 3,
    kOperationsStreamClosing = 4,
    kOperationsStreamClosed = 5
};

/*!
    \brief OperationsStream service for streaming operations data
    
    This class provides operations streaming using gRPC server-side streaming
    with callback-based async pattern.
    Supports:
    - Portfolio stream (real-time portfolio updates)
    - Positions stream (real-time position changes)
*/
class TINKOFFINVESTSDK_EXPORT OperationsStream : public CustomService
{
public:
    OperationsStream(std::shared_ptr<Channel> channel, const std::string &token);
    ~OperationsStream();
    
    // =========================================================================
    // Callback-based Async Streaming Methods
    // =========================================================================
    
    /*!
        \brief Subscribe to portfolio updates with callback
        \param accounts Vector of account IDs to subscribe to
        \param callback Callback function to receive portfolio responses
    */
    void PortfolioStreamAsync(
        const std::vector<std::string>& accounts,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to position changes with callback
        \param accounts Vector of account IDs to subscribe to
        \param callback Callback function to receive positions responses
    */
    void PositionsStreamAsync(
        const std::vector<std::string>& accounts,
        CallbackFunc callback);
    
    // =========================================================================
    // Stream lifecycle
    // =========================================================================
    
    bool isConnected() const;
    uint64_t getMessageCount() const;
    void close();
    
private:
    std::unique_ptr<OperationsStreamService::Stub> m_stub;
    std::shared_ptr<grpc::Channel> m_channel;
    std::string m_token;
    
    std::atomic<int> m_streamState{0};
    std::atomic<uint64_t> m_messageCount{0};
    std::atomic<bool> m_running{false};
    
    std::unique_ptr<grpc::ClientContext> m_context;
    std::thread m_streamThread;
    
    void addAuthMetadata(grpc::ClientContext &context);
    bool transitionState(int newState);
};

#endif // OPERATIONSSTREAMSERVICE_H

