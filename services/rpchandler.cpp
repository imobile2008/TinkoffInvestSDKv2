#include "rpchandler.h"
#include "ordersstreamresponse.h"

// Debug logging disabled by default - uncomment to enable
// #define DEBUG_RPCHANDLER

#ifdef DEBUG_RPCHANDLER
#include <iostream>
#include <chrono>

static std::string getTimestamp() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    time_t timer = time(nullptr);
    
    char buffer[32];
    strftime(buffer, sizeof(buffer), "%H:%M:%S", localtime(&timer));
    
    std::string result = buffer;
    result += "." + std::to_string(ms.count());
    return result;
}

#define RPCHANDLER_LOG(msg) std::cout << "[RPCHANDLER " << getTimestamp() << "] " << msg << std::endl
#else
#define RPCHANDLER_LOG(msg)
#endif

RpcHandler::RpcHandler(): tags(this)
{

}

void RpcHandler::handlingThread(CompletionQueue *cq)
{
    RPCHANDLER_LOG("Completion queue thread started");
    
    void *raw_tag = nullptr;
    bool ok = false;

    while (cq->Next(&raw_tag, &ok))
    {
        if (!raw_tag) {
            RPCHANDLER_LOG("Warning: Received null tag");
            continue;
        }

        TagData *tag = reinterpret_cast<TagData*>(raw_tag);
        if (!tag || !tag->handler) {
            RPCHANDLER_LOG("Invalid tag received");
            continue;
        }

        if (ok)
        {
            switch (tag->evt) {
                case TagData::Type::start_done:
                    RPCHANDLER_LOG("start_done event received");
                    tag->handler->on_ready();
                    break;
                case TagData::Type::read_done:
                    RPCHANDLER_LOG("read_done event received, ok=true");
                    tag->handler->on_recv();
                    break;
                case TagData::Type::write_done:
                    RPCHANDLER_LOG("write_done event received");
                    tag->handler->on_write_done();
                    break;
                case TagData::Type::finish_done:
                    RPCHANDLER_LOG("finish_done event received");
                    tag->handler->on_finish();
                    break;
                default:
                    RPCHANDLER_LOG("Unknown event type received: " << static_cast<int>(tag->evt));
                    break;
            }
        }
        else
        {
            // gRPC event failed - handle based on event type
            RPCHANDLER_LOG("Event FAILED with type: " << static_cast<int>(tag->evt));
            switch (tag->evt) {
                case TagData::Type::start_done:
                    RPCHANDLER_LOG("Connection failed - will retry or notify");
                    tag->handler->on_ready(); // Trigger to handle failure
                    break;
                case TagData::Type::read_done:
                    RPCHANDLER_LOG("Read failed - stream may be closed by server");
                    tag->handler->on_recv(); // Call to handle stream close
                    break;
                case TagData::Type::write_done:
                    RPCHANDLER_LOG("Write failed - check connection");
                    tag->handler->on_write_done();
                    break;
                case TagData::Type::finish_done:
                    RPCHANDLER_LOG("Finish event completed");
                    break;
                default:
                    RPCHANDLER_LOG("Unknown failed event type");
                    break;
            }
        }
    }
    RPCHANDLER_LOG("Completion queue loop ended - queue shutdown");
}

// ============================================================================
// MarketDataHandler Implementation
// ============================================================================

MarketDataHandler::MarketDataHandler(MarketDataHandler::responder_ptr responder, CallbackFunc callback)
    : responder_(std::move(responder)), callback_(callback)
{
    RPCHANDLER_LOG("MarketDataHandler constructor called (blocking version)");
    stream_state_.store(StreamState::kConnecting, std::memory_order_release);
    responder_->StartCall(&tags.start_done);
}

MarketDataHandler::MarketDataHandler(grpc::CompletionQueue &cq_, std::unique_ptr<MarketDataStreamService::Stub> &stub_, const std::string &token, CallbackFunc callback)
    : callback_(callback)
{
    RPCHANDLER_LOG("MarketDataHandler constructor called (async version)");
    stream_state_.store(StreamState::kConnecting, std::memory_order_release);

    std::string meta_value = "Bearer " + token;
    context.AddMetadata("authorization", meta_value);
    context.AddMetadata("x-app-name", APP_NAME);

    responder_ = stub_->PrepareAsyncMarketDataStream(&context, &cq_);
    RPCHANDLER_LOG("Starting async call");
    responder_->StartCall(&tags.start_done);
}

MarketDataHandler::~MarketDataHandler()
{
    RPCHANDLER_LOG("MarketDataHandler destroyed, messages received: " << message_count_.load());
}

bool MarketDataHandler::transitionState(StreamState newState)
{
    StreamState currentState = stream_state_.load(std::memory_order_acquire);
    StreamState expectedState = currentState;

    // Define valid state transitions
    bool valid = false;
    switch (newState) {
        case StreamState::kConnecting:
            valid = (currentState == StreamState::kDisconnected);
            break;
        case StreamState::kConnected:
            valid = (currentState == StreamState::kConnecting);
            break;
        case StreamState::kReceiving:
            valid = (currentState == StreamState::kConnected || currentState == StreamState::kReceiving);
            break;
        case StreamState::kClosing:
            valid = (currentState == StreamState::kReceiving || currentState == StreamState::kConnected);
            break;
        case StreamState::kClosed:
            valid = (currentState == StreamState::kClosing || currentState == StreamState::kConnecting ||
                     currentState == StreamState::kConnected || currentState == StreamState::kReceiving);
            break;
        default:
            break;
    }

    if (valid) {
        stream_state_.store(newState, std::memory_order_release);
        RPCHANDLER_LOG("State transition: " << stateToString(currentState) << " -> " << stateToString(newState));
    } else {
        RPCHANDLER_LOG("Invalid state transition attempted: " << stateToString(currentState) << " -> " << stateToString(newState));
    }

    return valid;
}

bool MarketDataHandler::hasValidPayload() const
{
    // Check if the incoming message has any data
    // For MarketDataResponse, check if it contains any subscription responses or data
    // This is a basic check - specific payloads may have different validation
    const auto& response = incoming_;

    // Check if the response has any content
    // MarketDataResponse contains optional fields for different subscription types
    bool hasContent = response.has_subscribe_candles_response() ||
                      response.has_subscribe_order_book_response() ||
                      response.has_subscribe_trades_response() ||
                      response.has_subscribe_info_response() ||
                      response.has_subscribe_last_price_response();

    // Allow first message even if empty (may contain subscription confirmation)
    // This prevents NULL pointer issues on initial connection
    return hasContent || message_count_.load() == 0;
}

void MarketDataHandler::send(const MarketDataRequest &msg)
{
    RPCHANDLER_LOG("send() called");
    
    if (ready_.load(std::memory_order_acquire) && !sending_.load(std::memory_order_acquire))
    {
        RPCHANDLER_LOG("Sending request immediately");
        sending_.store(true, std::memory_order_release);
        responder_->Write(msg, &tags.write_done);
    } else
    {
        RPCHANDLER_LOG("Queuing request (ready=" << ready_.load() << ", sending=" << sending_.load() << ")");
        queued_msgs_.push(msg);
    }
}

void MarketDataHandler::on_ready()
{
    RPCHANDLER_LOG("on_ready() called");

    // Transition to connected state
    if (!transitionState(StreamState::kConnected)) {
        RPCHANDLER_LOG("Failed to transition to CONNECTED state");
        return;
    }

    ready_.store(true, std::memory_order_release);
    RPCHANDLER_LOG("Handler ready, checking queued messages: " << queued_msgs_.size());

    if (!queued_msgs_.empty()) {
        sending_.store(true, std::memory_order_release);
        RPCHANDLER_LOG("Sending queued message");
        responder_->Write(queued_msgs_.front(), &tags.write_done);
        queued_msgs_.pop();
    }
    else
    {
        // Transition to receiving state and start reading
        RPCHANDLER_LOG("Starting to read from stream");
        transitionState(StreamState::kReceiving);
        responder_->Read(&incoming_, &tags.read_done);
    }
}

void MarketDataHandler::on_recv()
{
    RPCHANDLER_LOG("on_recv() called, state: " << stateToString(getStreamState()));

    // Check if this is a stream close event (ok=false handled in handlingThread)
    StreamState currentState = getStreamState();

    if (currentState == StreamState::kClosed ||
        currentState == StreamState::kClosing) {
        RPCHANDLER_LOG("Ignoring on_recv in closing/closed state");
        return;
    }

    // Validate payload before invoking callback
    bool validPayload = hasValidPayload();
    RPCHANDLER_LOG("Payload valid: " << validPayload << ", message count: " << message_count_.load());

    if (validPayload) {
        // Create MarketDataStreamResponse wrapper with automatic payload type detection
        MarketDataStreamResponse streamResponse(incoming_);

        auto data = ServiceReply(streamResponse, {});
        if (callback_) {
            message_count_.fetch_add(1, std::memory_order_relaxed);
            RPCHANDLER_LOG("Invoking callback with valid payload, message #" << message_count_.load());
            callback_(data);
        }
    } else {
        RPCHANDLER_LOG("Received empty payload, skipping callback");
    }

    // Continue reading if stream is still open
    if (currentState != StreamState::kClosing && currentState != StreamState::kClosed) {
        transitionState(StreamState::kReceiving);
        RPCHANDLER_LOG("Starting next read");
        responder_->Read(&incoming_, &tags.read_done);
    } else {
        RPCHANDLER_LOG("Stream is closing, not reading more");
    }
}

void MarketDataHandler::on_write_done()
{
    RPCHANDLER_LOG("on_write_done() called, queue size: " << queued_msgs_.size());

    if (!queued_msgs_.empty()) {
        RPCHANDLER_LOG("Sending next queued message");
        responder_->Write(queued_msgs_.front(), &tags.write_done);
        queued_msgs_.pop();
    } else {
        RPCHANDLER_LOG("All messages sent");
        sending_.store(false, std::memory_order_release);
        
        // If no pending writes and stream is ready, start reading
        if (ready_.load() && getStreamState() == StreamState::kConnected) {
            RPCHANDLER_LOG("Starting read after write done");
            transitionState(StreamState::kReceiving);
            responder_->Read(&incoming_, &tags.read_done);
        }
    }
}

void MarketDataHandler::on_finish()
{
    RPCHANDLER_LOG("on_finish() called, transitioning to CLOSED");
    transitionState(StreamState::kClosed);
}

// ============================================================================
// OrdersHandler Implementation
// ============================================================================

OrdersHandler::OrdersHandler(OrdersHandler::responder_ptr responder, CallbackFunc callback)
    : responder_(std::move(responder)), callback_(callback)
{
    RPCHANDLER_LOG("OrdersHandler constructor called (blocking version)");
    stream_state_.store(StreamState::kConnecting, std::memory_order_release);
    responder_->StartCall(&tags.start_done);
}

OrdersHandler::OrdersHandler(grpc::CompletionQueue &cq_, std::unique_ptr<OrdersStreamService::Stub> &stub_, const std::string &token, TradesStreamRequest &request, CallbackFunc callback)
    : callback_(callback)
{
    RPCHANDLER_LOG("OrdersHandler constructor called (async version)");
    stream_state_.store(StreamState::kConnecting, std::memory_order_release);

    std::string meta_value = "Bearer " + token;
    context.AddMetadata("authorization", meta_value);
    context.AddMetadata("x-app-name", APP_NAME);

    responder_ = stub_->PrepareAsyncTradesStream(&context, request, &cq_);
    RPCHANDLER_LOG("Starting async trades stream");
    responder_->StartCall(&tags.start_done);
}

OrdersHandler::~OrdersHandler()
{
    RPCHANDLER_LOG("OrdersHandler destroyed, messages received: " << message_count_.load());
}

bool OrdersHandler::transitionState(StreamState newState)
{
    StreamState currentState = stream_state_.load(std::memory_order_acquire);

    // Define valid state transitions for OrdersHandler
    bool valid = false;
    switch (newState) {
        case StreamState::kConnecting:
            valid = (currentState == StreamState::kDisconnected);
            break;
        case StreamState::kConnected:
            valid = (currentState == StreamState::kConnecting);
            break;
        case StreamState::kReceiving:
            valid = (currentState == StreamState::kConnected || currentState == StreamState::kReceiving);
            break;
        case StreamState::kClosing:
            valid = (currentState == StreamState::kReceiving || currentState == StreamState::kConnected);
            break;
        case StreamState::kClosed:
            valid = (currentState == StreamState::kClosing || currentState == StreamState::kConnecting ||
                     currentState == StreamState::kConnected || currentState == StreamState::kReceiving);
            break;
        default:
            break;
    }

    if (valid) {
        stream_state_.store(newState, std::memory_order_release);
        RPCHANDLER_LOG("State transition: " << stateToString(currentState) << " -> " << stateToString(newState));
    } else {
        RPCHANDLER_LOG("Invalid state transition attempted: " << stateToString(currentState) << " -> " << stateToString(newState));
    }

    return valid;
}

bool OrdersHandler::hasValidPayload() const
{
    // Check if the incoming TradesStreamResponse has any data
    const auto& response = incoming_;

    // Check for order trades presence using has_* methods
    // The TradesStreamResponse has an optional order_trades field
    bool hasOrderTrades = response.has_order_trades();

    // Also allow first message even if empty (may contain account info)
    // This prevents NULL pointer issues on initial connection
    return hasOrderTrades || message_count_.load() == 0;
}

void OrdersHandler::on_ready()
{
    RPCHANDLER_LOG("OrdersHandler::on_ready called");

    // Transition to connected state
    if (!transitionState(StreamState::kConnected)) {
        RPCHANDLER_LOG("Failed to transition to CONNECTED state");
        return;
    }

    // Transition to receiving and start reading
    transitionState(StreamState::kReceiving);
    RPCHANDLER_LOG("Starting to read trades stream");
    responder_->Read(&incoming_, &tags.read_done);
}

void OrdersHandler::on_recv()
{
    RPCHANDLER_LOG("OrdersHandler::on_recv called, state: " << stateToString(getStreamState()));

    // Check if this is a stream close event
    StreamState currentState = getStreamState();

    if (currentState == StreamState::kClosed ||
        currentState == StreamState::kClosing) {
        RPCHANDLER_LOG("Ignoring on_recv in closing/closed state");
        return;
    }

    // Validate payload before invoking callback
    bool validPayload = hasValidPayload();
    RPCHANDLER_LOG("Payload valid: " << validPayload << ", message count: " << message_count_.load());

    if (validPayload) {
        // Create OrdersStreamResponse wrapper for proper callback handling
        OrdersStreamResponse streamResponse(incoming_);
        
        auto data = ServiceReply(streamResponse, {});
        if (callback_) {
            message_count_.fetch_add(1, std::memory_order_relaxed);
            RPCHANDLER_LOG("Invoking callback with valid payload, message #" << message_count_.load());
            callback_(data);
        }
    } else {
        RPCHANDLER_LOG("Received empty payload, skipping callback");
    }

    // Continue reading if stream is still open
    if (currentState != StreamState::kClosing && currentState != StreamState::kClosed) {
        transitionState(StreamState::kReceiving);
        RPCHANDLER_LOG("Starting next read");
        responder_->Read(&incoming_, &tags.read_done);
    } else {
        RPCHANDLER_LOG("Stream is closing, not reading more");
    }
}

void OrdersHandler::on_write_done()
{
    RPCHANDLER_LOG("OrdersHandler::on_write_done called");
    // Orders stream is read-only, no write operations
}

void OrdersHandler::on_finish()
{
    RPCHANDLER_LOG("OrdersHandler::on_finish called, transitioning to CLOSED");
    transitionState(StreamState::kClosed);
    
    // Call the finish callback if set
    if (finishCallback_) {
        finishCallback_();
    }
}

