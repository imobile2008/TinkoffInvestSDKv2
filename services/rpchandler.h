#ifndef RPCHANDLER_H
#define RPCHANDLER_H

#include <queue>
#include <atomic>
#include <functional>
#include <grpcpp/grpcpp.h>
#include "marketdata.grpc.pb.h"
#include "orders.grpc.pb.h"
#include "customservice.h"
#include "commontypes.h"

using namespace tinkoff::public_::invest::api::contract::v1;

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientAsyncReader;

/*!
    \brief  Базовый класс для асинхронных двунаправленных RPC вызовов

    Поток обработки не будет ассоциирован с конкрентным RPC методом.
*/
class RpcHandler {

    struct TagData
    {
        enum class Type {
            start_done,
            read_done,
            write_done,
            finish_done
        };

        RpcHandler *handler;
        Type evt;
    };

    struct TagSet
    {
        TagSet(RpcHandler *self)
            : start_done {self, TagData::Type::start_done},
              read_done {self, TagData::Type::read_done},
              write_done {self, TagData::Type::write_done},
              finish_done {self, TagData::Type::finish_done} {}
        TagData start_done;
        TagData read_done;
        TagData write_done;
        TagData finish_done;
    };

public:

    RpcHandler();
    virtual ~RpcHandler() = default;

    TagSet tags;
    ClientContext context;

    virtual void on_ready() = 0;
    virtual void on_recv() = 0;
    virtual void on_write_done() = 0;
    virtual void on_finish() = 0;

    static void handlingThread(CompletionQueue *cq);

protected:
    // Stream state enum for lifecycle tracking
    enum class StreamState {
        kDisconnected = 0,
        kConnecting = 1,
        kConnected = 2,
        kReceiving = 3,
        kClosing = 4,
        kClosed = 5
    };

    // Get human-readable state name for logging
    static const char* stateToString(StreamState state) {
        switch (state) {
            case StreamState::kDisconnected: return "DISCONNECTED";
            case StreamState::kConnecting: return "CONNECTING";
            case StreamState::kConnected: return "CONNECTED";
            case StreamState::kReceiving: return "RECEIVING";
            case StreamState::kClosing: return "CLOSING";
            case StreamState::kClosed: return "CLOSED";
            default: return "UNKNOWN";
        }
    }
};

/*!
    \brief Обработчик асинхронных двунаправленных RPC вызовов MarketDataStream сервиса
*/
class MarketDataHandler final : public RpcHandler
{

public:
    using responder_ptr = std::unique_ptr<ClientAsyncReaderWriter<MarketDataRequest, MarketDataResponse>>;

    MarketDataHandler(responder_ptr responder, CallbackFunc callback);
    MarketDataHandler(CompletionQueue &cq_, std::unique_ptr<MarketDataStreamService::Stub> &stub_, const std::string &token, CallbackFunc callback);
    ~MarketDataHandler();

    void send(const MarketDataRequest &msg);

    // State accessors for external monitoring
    StreamState getStreamState() const { return stream_state_.load(std::memory_order_acquire); }
    uint64_t getMessageCount() const { return message_count_.load(std::memory_order_relaxed); }
    bool isConnected() const { return stream_state_.load(std::memory_order_acquire) >= StreamState::kConnected; }
    bool isReceiving() const { return stream_state_.load(std::memory_order_acquire) == StreamState::kReceiving; }

private:
    void on_ready() override;
    void on_recv() override;
    void on_write_done() override;
    void on_finish() override;

    // Internal state transition method
    bool transitionState(StreamState newState);

    // Validate that the incoming message contains valid data
    bool hasValidPayload() const;

    responder_ptr responder_;
    MarketDataResponse incoming_;

    std::atomic<StreamState> stream_state_{StreamState::kDisconnected};
    std::atomic<uint64_t> message_count_{0};
    std::atomic<bool> sending_{false};
    std::atomic<bool> ready_{false};
    std::queue<MarketDataRequest> queued_msgs_;
    CallbackFunc callback_;

};

/*!
    \brief Обработчик асинхронных однонаправленных RPC вызовов OrdersStream сервиса
*/
class OrdersHandler final : public RpcHandler
{

public:
    using responder_ptr = std::unique_ptr<ClientAsyncReader<TradesStreamResponse>>;

    OrdersHandler(responder_ptr responder, std::function<void (ServiceReply)> callback);
    OrdersHandler(CompletionQueue &cq_, std::unique_ptr<OrdersStreamService::Stub> &stub_, const std::string &token, TradesStreamRequest &request, CallbackFunc callback);
    ~OrdersHandler();

    // Set a callback to be called when stream finishes
    void setFinishCallback(std::function<void()> callback) { finishCallback_ = callback; }

    // State accessors for external monitoring
    StreamState getStreamState() const { return stream_state_.load(std::memory_order_acquire); }
    uint64_t getMessageCount() const { return message_count_.load(std::memory_order_relaxed); }
    bool isConnected() const { return stream_state_.load(std::memory_order_acquire) >= StreamState::kConnected; }
    bool isReceiving() const { return stream_state_.load(std::memory_order_acquire) == StreamState::kReceiving; }
    
    // Cancel the ongoing operation
    void cancel() { context.TryCancel(); }

private:
    void on_ready() override;
    void on_recv() override;
    void on_write_done() override;
    void on_finish() override;

    // Internal state transition method
    bool transitionState(StreamState newState);

    // Validate that the incoming message contains valid data
    bool hasValidPayload() const;

    responder_ptr responder_;
    TradesStreamResponse incoming_;

    std::atomic<StreamState> stream_state_{StreamState::kDisconnected};
    std::atomic<uint64_t> message_count_{0};
    CallbackFunc callback_;
    std::function<void()> finishCallback_;

};

#endif // RPCHANDLER_H

