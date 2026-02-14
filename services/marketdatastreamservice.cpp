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
    if (m_streamThread.joinable()) {
        m_streamThread.join();
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
// C++20 Coroutine-based Streaming Methods
// ============================================================================

// These methods use StreamChannel for thread-safe communication between
// the background streaming thread and the coroutine consumer.

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeCandlesCoroutine(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments)
{
    // Validate input
    if (candleInstruments.empty()) {
        std::cerr << "[MarketDataStream] SubscribeCandlesCoroutine: Error - candleInstruments is empty" << std::endl;
        co_return;
    }
    
    // Prevent multiple concurrent subscriptions
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeCandlesCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    // Create channel for streaming data
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    
    m_running.store(true);
    
    // Run streaming in background thread
    m_streamThread = std::thread([this, candleInstruments, channel]() {
        try {
            MarketDataRequest request = createCandlesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, candleInstruments);
            
            // Transition to connecting state
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            // Create stream context
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            
            // Create bidirectional stream
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            // Write subscription request
            m_stream->Write(request);
            transitionState(kStreaming);
            
            // Read responses and send to channel
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break; // Channel closed by consumer
                }
            }
            
            // Finish stream
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeCandlesCoroutine: Exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[MarketDataStream] SubscribeCandlesCoroutine: Unknown exception" << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    // Yield responses from channel
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break; // Channel closed
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeLastPriceCoroutine(
    const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeLastPriceCoroutine: Error - instrumentIds is empty" << std::endl;
        co_return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeLastPriceCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, channel]() {
        try {
            MarketDataRequest request = createLastPriceRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break;
                }
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeLastPriceCoroutine: Exception: " << e.what() << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break;
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeTradesCoroutine(
    const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeTradesCoroutine: Error - instrumentIds is empty" << std::endl;
        co_return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeTradesCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, channel]() {
        try {
            MarketDataRequest request = createTradesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break;
                }
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeTradesCoroutine: Exception: " << e.what() << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break;
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeOrderBookCoroutine(
    const std::vector<std::string>& instrumentIds, int32_t depth)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeOrderBookCoroutine: Error - instrumentIds is empty" << std::endl;
        co_return;
    }
    
    if (depth <= 0) {
        depth = 10;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeOrderBookCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, depth, channel]() {
        try {
            MarketDataRequest request = createOrderBookRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds, depth);
            
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break;
                }
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeOrderBookCoroutine: Exception: " << e.what() << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break;
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeInfoCoroutine(
    const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeInfoCoroutine: Error - instrumentIds is empty" << std::endl;
        co_return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeInfoCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, channel]() {
        try {
            MarketDataRequest request = createInfoRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break;
                }
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeInfoCoroutine: Exception: " << e.what() << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break;
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}

MarketDataStreamGenerator<MarketDataStreamResponse> MarketDataStream::SubscribeAllCoroutine(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    const std::vector<std::string>& orderBookInstruments,
    int32_t orderBookDepth,
    const std::vector<std::string>& tradesInstruments,
    const std::vector<std::string>& infoInstruments,
    const std::vector<std::string>& lastPriceInstruments)
{
    bool hasAnySubscription = !candleInstruments.empty() || !orderBookInstruments.empty() || 
                              !tradesInstruments.empty() || !infoInstruments.empty() || 
                              !lastPriceInstruments.empty();
    
    if (!hasAnySubscription) {
        std::cerr << "[MarketDataStream] SubscribeAllCoroutine: Error - no instruments to subscribe" << std::endl;
        co_return;
    }
    
    if (orderBookDepth <= 0) {
        orderBookDepth = 10;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeAllCoroutine: Error - stream already running" << std::endl;
        co_return;
    }
    
    auto channel = std::make_shared<StreamChannel<MarketDataStreamResponse>>();
    m_running.store(true);
    
    m_streamThread = std::thread([this, candleInstruments, orderBookInstruments, orderBookDepth,
                                  tradesInstruments, infoInstruments, lastPriceInstruments, channel]() {
        try {
            // Inline combined request creation (logic moved from removed createCombinedRequest method)
            MarketDataRequest request;
            
            // Add candles subscription
            if (!candleInstruments.empty()) {
                auto* scr = request.mutable_subscribe_candles_request();
                scr->set_subscription_action(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE);
                for (const auto& [instrumentId, interval] : candleInstruments) {
                    auto* instr = scr->add_instruments();
                    instr->set_instrument_id(instrumentId);
                    instr->set_interval(interval);
                }
            }
            
            // Add order book subscription
            if (!orderBookInstruments.empty()) {
                auto* sobr = request.mutable_subscribe_order_book_request();
                sobr->set_subscription_action(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE);
                for (const auto& instrumentId : orderBookInstruments) {
                    auto* instr = sobr->add_instruments();
                    instr->set_instrument_id(instrumentId);
                    instr->set_depth(orderBookDepth);
                }
            }
            
            // Add trades subscription
            if (!tradesInstruments.empty()) {
                auto* str = request.mutable_subscribe_trades_request();
                str->set_subscription_action(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE);
                for (const auto& instrumentId : tradesInstruments) {
                    auto* instr = str->add_instruments();
                    instr->set_instrument_id(instrumentId);
                }
            }
            
            // Add info subscription
            if (!infoInstruments.empty()) {
                auto* sir = request.mutable_subscribe_info_request();
                sir->set_subscription_action(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE);
                for (const auto& instrumentId : infoInstruments) {
                    auto* instr = sir->add_instruments();
                    instr->set_instrument_id(instrumentId);
                }
            }
            
            // Add last price subscription
            if (!lastPriceInstruments.empty()) {
                auto* slpr = request.mutable_subscribe_last_price_request();
                slpr->set_subscription_action(SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE);
                for (const auto& instrumentId : lastPriceInstruments) {
                    auto* instr = slpr->add_instruments();
                    instr->set_instrument_id(instrumentId);
                }
            }
            
            if (!transitionState(kConnecting)) {
                channel->close();
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                channel->close();
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                if (!channel->send(std::move(streamResponse))) {
                    break;
                }
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeAllCoroutine: Exception: " << e.what() << std::endl;
        }
        
        channel->close();
        m_running.store(false);
    });
    
    while (true) {
        MarketDataStreamResponse response;
        if (channel->receive(response)) {
            co_yield std::move(response);
        } else {
            break;
        }
    }
    
    // Note: Background thread handles cleanup - do not call close() here
}


// ============================================================================
// Callback-based Async Streaming Methods (non-coroutine)
// ============================================================================

void MarketDataStream::SubscribeCandlesAsync(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    CallbackFunc callback)
{
    if (candleInstruments.empty()) {
        std::cerr << "[MarketDataStream] SubscribeCandlesAsync: Error - candleInstruments is empty" << std::endl;
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeCandlesAsync: Error - stream already running" << std::endl;
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, candleInstruments, callback]() {
        try {
            MarketDataRequest request = createCandlesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, candleInstruments);
            
            if (!transitionState(kConnecting)) {
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                callback(ServiceReply(streamResponse));
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeCandlesAsync: Exception: " << e.what() << std::endl;
        }
        
        m_running.store(false);
    });
}

void MarketDataStream::SubscribeLastPriceAsync(
    const std::vector<std::string>& instrumentIds,
    CallbackFunc callback)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeLastPriceAsync: Error - instrumentIds is empty" << std::endl;
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeLastPriceAsync: Error - stream already running" << std::endl;
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, callback]() {
        try {
            MarketDataRequest request = createLastPriceRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                callback(ServiceReply(streamResponse));
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeLastPriceAsync: Exception: " << e.what() << std::endl;
        }
        
        m_running.store(false);
    });
}

void MarketDataStream::SubscribeTradesAsync(
    const std::vector<std::string>& instrumentIds,
    CallbackFunc callback)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeTradesAsync: Error - instrumentIds is empty" << std::endl;
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeTradesAsync: Error - stream already running" << std::endl;
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, callback]() {
        try {
            MarketDataRequest request = createTradesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                callback(ServiceReply(streamResponse));
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeTradesAsync: Exception: " << e.what() << std::endl;
        }
        
        m_running.store(false);
    });
}

void MarketDataStream::SubscribeOrderBookAsync(
    const std::vector<std::string>& instrumentIds,
    int32_t depth,
    CallbackFunc callback)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeOrderBookAsync: Error - instrumentIds is empty" << std::endl;
        return;
    }
    
    if (depth <= 0) {
        depth = 10;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeOrderBookAsync: Error - stream already running" << std::endl;
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, depth, callback]() {
        try {
            MarketDataRequest request = createOrderBookRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds, depth);
            
            if (!transitionState(kConnecting)) {
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                callback(ServiceReply(streamResponse));
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeOrderBookAsync: Exception: " << e.what() << std::endl;
        }
        
        m_running.store(false);
    });
}

void MarketDataStream::SubscribeInfoAsync(
    const std::vector<std::string>& instrumentIds,
    CallbackFunc callback)
{
    if (instrumentIds.empty()) {
        std::cerr << "[MarketDataStream] SubscribeInfoAsync: Error - instrumentIds is empty" << std::endl;
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[MarketDataStream] SubscribeInfoAsync: Error - stream already running" << std::endl;
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, instrumentIds, callback]() {
        try {
            MarketDataRequest request = createInfoRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentIds);
            
            if (!transitionState(kConnecting)) {
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            m_stream = m_stub->MarketDataStream(m_context.get());
            
            if (!transitionState(kConnected)) {
                return;
            }
            
            m_stream->Write(request);
            transitionState(kStreaming);
            
            MarketDataResponse response;
            while (m_stream->Read(&response)) {
                m_messageCount.fetch_add(1);
                MarketDataStreamResponse streamResponse(response);
                callback(ServiceReply(streamResponse));
            }
            
            m_stream->Finish();
            transitionState(kClosed);
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeInfoAsync: Exception: " << e.what() << std::endl;
        }
        
        m_running.store(false);
    });
}

void MarketDataStream::UnSubscribeCandlesAsync()
{
    if (!m_running.load()) {
        return;
    }
    
    try {
        MarketDataRequest request = createCandlesRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeCandlesAsync: Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeLastPriceAsync()
{
    if (!m_running.load()) {
        return;
    }
    
    try {
        MarketDataRequest request = createLastPriceRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeLastPriceAsync: Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeTradesAsync()
{
    if (!m_running.load()) {
        return;
    }
    
    try {
        MarketDataRequest request = createTradesRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeTradesAsync: Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeOrderBookAsync()
{
    if (!m_running.load()) {
        return;
    }
    
    try {
        MarketDataRequest request = createOrderBookRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {}, 0);
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeOrderBookAsync: Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeInfoAsync()
{
    if (!m_running.load()) {
        return;
    }
    
    try {
        MarketDataRequest request = createInfoRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeInfoAsync: Exception: " << e.what() << std::endl;
    }
}
