#include "marketdatastreamservice.h"
#include <iostream>
#include <thread>

using grpc::ClientReaderWriter;

// ============================================================================
// MarketDataStream Implementation
// ============================================================================

MarketDataStream::MarketDataStream(std::shared_ptr<grpc::Channel> channel, const std::string &token)
    : CustomService(token)
    , m_stub(MarketDataStreamService::NewStub(channel))
    , m_channel(std::move(channel))
    , m_token(token)
    , m_streamState(0)
    , m_messageCount(0)
    , m_running(false)
{
    std::cout << "[MarketDataStream] Constructor called" << std::endl;
}

MarketDataStream::~MarketDataStream()
{
    std::cout << "[MarketDataStream] Destructor called" << std::endl;
    close();
    if (m_grpcThread) {
        m_grpcThread->join();
    }
}

void MarketDataStream::addAuthMetadata(grpc::ClientContext &context)
{
    std::string meta_value = "Bearer " + m_token;
    context.AddMetadata("authorization", meta_value);
    context.AddMetadata("x-app-name", APP_NAME);
}

bool MarketDataStream::transitionState(int newState)
{
    int currentState = m_streamState.load();
    bool valid = false;
    
    switch (newState) {
        case kConnecting: valid = (currentState == kDisconnected); break;
        case kConnected: valid = (currentState == kConnecting); break;
        case kStreaming: valid = (currentState == kConnected); break;
        case kClosing: valid = (currentState == kStreaming || currentState == kConnected || currentState == kConnecting); break;
        case kClosed: valid = (currentState == kClosing || currentState == kConnecting || currentState == kDisconnected); break;
        default: break;
    }
    
    if (valid) {
        m_streamState.store(newState);
        std::cout << "[MarketDataStream] State: " << currentState << " -> " << newState << std::endl;
    }
    
    return valid;
}

MarketDataRequest MarketDataStream::createCandlesRequest(SubscriptionAction action, const std::vector<std::pair<std::string, SubscriptionInterval>>& instruments)
{
    MarketDataRequest request;
    auto* scr = request.mutable_subscribe_candles_request();
    scr->set_subscription_action(action);
    for (const auto& [instrumentId, interval] : instruments) {
        auto* instr = scr->add_instruments();
        instr->set_instrument_id(instrumentId);
        instr->set_interval(interval);
    }
    return request;
}

MarketDataRequest MarketDataStream::createOrderBookRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds, int32_t depth)
{
    MarketDataRequest request;
    auto* sobr = request.mutable_subscribe_order_book_request();
    sobr->set_subscription_action(action);
    for (const auto& instrumentId : instrumentIds) {
        auto* instr = sobr->add_instruments();
        instr->set_instrument_id(instrumentId);
        instr->set_depth(depth);
    }
    return request;
}

MarketDataRequest MarketDataStream::createTradesRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds)
{
    MarketDataRequest request;
    auto* str = request.mutable_subscribe_trades_request();
    str->set_subscription_action(action);
    for (const auto& instrumentId : instrumentIds) {
        auto* instr = str->add_instruments();
        instr->set_instrument_id(instrumentId);
    }
    return request;
}

MarketDataRequest MarketDataStream::createInfoRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds)
{
    MarketDataRequest request;
    auto* sir = request.mutable_subscribe_info_request();
    sir->set_subscription_action(action);
    for (const auto& instrumentId : instrumentIds) {
        auto* instr = sir->add_instruments();
        instr->set_instrument_id(instrumentId);
    }
    return request;
}

MarketDataRequest MarketDataStream::createLastPriceRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds)
{
    MarketDataRequest request;
    auto* slpr = request.mutable_subscribe_last_price_request();
    slpr->set_subscription_action(action);
    for (const auto& instrumentId : instrumentIds) {
        auto* instr = slpr->add_instruments();
        instr->set_instrument_id(instrumentId);
    }
    return request;
}

void MarketDataStream::close()
{
    std::cout << "[MarketDataStream] close() called" << std::endl;
    
    // First, signal the streaming to stop to prevent new callbacks
    bool expected = true;
    if (m_running.compare_exchange_strong(expected, false)) {
        // Cancel the gRPC context to interrupt any blocking reads
        if (m_context) {
            m_context->TryCancel();
        }
        
        // Wait for the stream thread to finish
        if (m_streamThread.joinable()) {
            m_streamThread.join();
        }
    }
    
    // Clean up stream resources
    if (m_stream) {
        m_stream->WritesDone();
        m_stream.reset();
    }
    
    m_context.reset();
    transitionState(kClosed);
    std::cout << "[MarketDataStream] close() completed" << std::endl;
}

bool MarketDataStream::isConnected() const
{
    return m_streamState.load() >= kConnected;
}

uint64_t MarketDataStream::getMessageCount() const
{
    return m_messageCount.load();
}

// ============================================================================
// Template implementation for streaming loop
// ============================================================================

template<typename RequestType>
void MarketDataStream::streamLoop(const MarketDataRequest& request, CallbackFunc callback)
{
    std::cout << "[MarketDataStream] streamLoop started" << std::endl;
    
    // Transition to connecting state
    if (!transitionState(kConnecting)) {
        return;
    }
    
    // Create stream context
    m_context = std::make_unique<grpc::ClientContext>();
    addAuthMetadata(*m_context);
    
    // Create bidirectional stream
    m_stream = m_stub->MarketDataStream(m_context.get());
    
    if (!transitionState(kConnected)) {
        return;
    }
    
    // Write subscription request
    m_stream->Write(request);
    
    // Transition to streaming state
    transitionState(kStreaming);
    
    // Read responses in a loop
    MarketDataResponse response;
    while (m_stream->Read(&response)) {
        try {
            m_messageCount.fetch_add(1);
            std::cout << "[MarketDataStream] Received message #" << m_messageCount.load() << std::endl;
            
            // Create the response wrapper with automatic payload type detection
            MarketDataStreamResponse streamResponse(response);
            
            // Log the detected payload type for debugging
            std::cout << "[MarketDataStream] Stream response type: " 
                      << streamResponse.getPayloadTypeString() << std::endl;
            
            // Invoke callback with the properly constructed ServiceReply
            if (callback) {
                callback(ServiceReply(streamResponse, grpc::Status()));
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] Error processing response: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[MarketDataStream] Unknown error processing response" << std::endl;
        }
    }
    
    // Read finished, clean up
    grpc::Status status = m_stream->Finish();
    std::cout << "[MarketDataStream] Stream finished, status: " << status.ok() << std::endl;
    
    transitionState(kClosed);
}

// ============================================================================
// Async Subscription Methods
// ============================================================================

void MarketDataStream::SubscribeCandles(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, candleInstruments, callback]() mutable {
        MarketDataRequest request = createCandlesRequest(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, candleInstruments);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

void MarketDataStream::SubscribeLastPrice(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, instrumentIds, callback]() mutable {
        MarketDataRequest request = createLastPriceRequest(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

void MarketDataStream::SubscribeTrades(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, instrumentIds, callback]() mutable {
        MarketDataRequest request = createTradesRequest(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

void MarketDataStream::SubscribeOrderBook(const std::vector<std::string> &instrumentIds, int32_t depth, CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, instrumentIds, depth, callback]() mutable {
        MarketDataRequest request = createOrderBookRequest(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds, depth);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

void MarketDataStream::SubscribeInfo(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, instrumentIds, callback]() mutable {
        MarketDataRequest request = createInfoRequest(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

// ============================================================================
// Async Methods (already async)
// ============================================================================

void MarketDataStream::SubscribeCandlesAsync(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback)
{
    SubscribeCandles(candleInstruments, callback);
}

void MarketDataStream::SubscribeOrderBookAsync(const std::vector<std::string> &instrumentIds, int32_t depth, CallbackFunc callback)
{
    SubscribeOrderBook(instrumentIds, depth, callback);
}

void MarketDataStream::SubscribeTradesAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    SubscribeTrades(instrumentIds, callback);
}

void MarketDataStream::SubscribeInfoAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    SubscribeInfo(instrumentIds, callback);
}

void MarketDataStream::SubscribeLastPriceAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback)
{
    SubscribeLastPrice(instrumentIds, callback);
}

// ============================================================================
// Unsubscription Methods
// ============================================================================

void MarketDataStream::UnSubscribeCandles()
{
    MarketDataRequest request = createCandlesRequest(SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
    if (m_stream) m_stream->Write(request);
}

void MarketDataStream::UnSubscribeOrderBook()
{
    MarketDataRequest request = createOrderBookRequest(SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {}, 0);
    if (m_stream) m_stream->Write(request);
}

void MarketDataStream::UnSubscribeTrades()
{
    MarketDataRequest request = createTradesRequest(SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
    if (m_stream) m_stream->Write(request);
}

void MarketDataStream::UnSubscribeLastPrice()
{
    MarketDataRequest request = createLastPriceRequest(SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
    if (m_stream) m_stream->Write(request);
}

void MarketDataStream::UnSubscribeInfo()
{
    MarketDataRequest request = createInfoRequest(SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
    if (m_stream) m_stream->Write(request);
}

// ============================================================================
// Async Unsubscription Methods
// ============================================================================

void MarketDataStream::UnSubscribeCandlesAsync() { UnSubscribeCandles(); }
void MarketDataStream::UnSubscribeOrderBookAsync() { UnSubscribeOrderBook(); }
void MarketDataStream::UnSubscribeTradesAsync() { UnSubscribeTrades(); }
void MarketDataStream::UnSubscribeLastPriceAsync() { UnSubscribeLastPrice(); }
void MarketDataStream::UnSubscribeInfoAsync() { UnSubscribeInfo(); }

// ============================================================================
// Combined Subscription Methods (Single Stream)
// ============================================================================

MarketDataRequest MarketDataStream::createCombinedRequest(
    SubscriptionAction action,
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    const std::vector<std::string>& orderBookInstruments,
    int32_t orderBookDepth,
    const std::vector<std::string>& tradesInstruments,
    const std::vector<std::string>& infoInstruments,
    const std::vector<std::string>& lastPriceInstruments)
{
    MarketDataRequest request;
    
    // Add candles subscription
    if (!candleInstruments.empty()) {
        auto* scr = request.mutable_subscribe_candles_request();
        scr->set_subscription_action(action);
        for (const auto& [instrumentId, interval] : candleInstruments) {
            auto* instr = scr->add_instruments();
            instr->set_instrument_id(instrumentId);
            instr->set_interval(interval);
        }
    }
    
    // Add order book subscription
    if (!orderBookInstruments.empty()) {
        auto* sobr = request.mutable_subscribe_order_book_request();
        sobr->set_subscription_action(action);
        for (const auto& instrumentId : orderBookInstruments) {
            auto* instr = sobr->add_instruments();
            instr->set_instrument_id(instrumentId);
            instr->set_depth(orderBookDepth);
        }
    }
    
    // Add trades subscription
    if (!tradesInstruments.empty()) {
        auto* str = request.mutable_subscribe_trades_request();
        str->set_subscription_action(action);
        for (const auto& instrumentId : tradesInstruments) {
            auto* instr = str->add_instruments();
            instr->set_instrument_id(instrumentId);
        }
    }
    
    // Add info subscription
    if (!infoInstruments.empty()) {
        auto* sir = request.mutable_subscribe_info_request();
        sir->set_subscription_action(action);
        for (const auto& instrumentId : infoInstruments) {
            auto* instr = sir->add_instruments();
            instr->set_instrument_id(instrumentId);
        }
    }
    
    // Add last price subscription
    if (!lastPriceInstruments.empty()) {
        auto* slpr = request.mutable_subscribe_last_price_request();
        slpr->set_subscription_action(action);
        for (const auto& instrumentId : lastPriceInstruments) {
            auto* instr = slpr->add_instruments();
            instr->set_instrument_id(instrumentId);
        }
    }
    
    return request;
}

void MarketDataStream::SubscribeAll(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    const std::vector<std::string>& orderBookInstruments,
    int32_t orderBookDepth,
    const std::vector<std::string>& tradesInstruments,
    const std::vector<std::string>& infoInstruments,
    const std::vector<std::string>& lastPriceInstruments,
    CallbackFunc callback)
{
    m_running.store(true);
    m_streamThread = std::thread([this, candleInstruments, orderBookInstruments, orderBookDepth, 
                                   tradesInstruments, infoInstruments, lastPriceInstruments, callback]() mutable {
        MarketDataRequest request = createCombinedRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE,
            candleInstruments, orderBookInstruments, orderBookDepth,
            tradesInstruments, infoInstruments, lastPriceInstruments);
        streamLoop<MarketDataRequest>(request, callback);
    });
}

void MarketDataStream::SubscribeAllAsync(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    const std::vector<std::string>& orderBookInstruments,
    int32_t orderBookDepth,
    const std::vector<std::string>& tradesInstruments,
    const std::vector<std::string>& infoInstruments,
    const std::vector<std::string>& lastPriceInstruments,
    CallbackFunc callback)
{
    SubscribeAll(candleInstruments, orderBookInstruments, orderBookDepth,
                tradesInstruments, infoInstruments, lastPriceInstruments, callback);
}

void MarketDataStream::UnSubscribeAll()
{
    MarketDataRequest request = createCombinedRequest(
        SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE,
        {}, {}, 0, {}, {}, {});
    if (m_stream) m_stream->Write(request);
}

void MarketDataStream::UnSubscribeAllAsync()
{
    UnSubscribeAll();
}

