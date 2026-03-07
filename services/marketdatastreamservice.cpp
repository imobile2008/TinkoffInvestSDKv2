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
// Callback-based Async Streaming Methods
// ============================================================================

void MarketDataStream::SubscribeCandlesAsync(
    const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
    CallbackFunc callback)
{
    if (candleInstruments.empty()) {
        std::cerr << "[MarketDataStream] SubscribeCandlesAsync: Error - candleInstruments is empty" << std::endl;
        return;
    }
    
    std::vector<std::pair<std::string, SubscriptionInterval>> newInstruments;
    
    {
        std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
        
        // Add new instruments to the tracking set
        for (const auto& [instrumentId, interval] : candleInstruments) {
            if (m_subscribedCandles.insert({instrumentId, interval}).second) {
                newInstruments.push_back({instrumentId, interval});
            }
        }
        
        if (newInstruments.empty()) {
            std::cout << "[MarketDataStream] SubscribeCandlesAsync: All instruments already subscribed" << std::endl;
            return;
        }
        
        std::cout << "[MarketDataStream] SubscribeCandlesAsync: Adding " << newInstruments.size() 
                  << " new instruments, total: " << m_subscribedCandles.size() << std::endl;
    }
    
    if (m_running.load()) {
        // Stream already running - send subscribe request for new instruments
        try {
            MarketDataRequest request = createCandlesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, newInstruments);
            
            if (m_stream) {
                m_stream->Write(request);
                std::cout << "[MarketDataStream] SubscribeCandlesAsync: Added instruments to existing stream" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeCandlesAsync: Exception during add: " << e.what() << std::endl;
        }
        return;
    }
    
    // Start new stream with all subscribed instruments
    m_running.store(true);
    
    m_streamThread = std::thread([this, callback]() {
        try {
            // Get current subscriptions
            std::set<std::pair<std::string, SubscriptionInterval>> subscribedInstruments;
            {
                std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
                subscribedInstruments = m_subscribedCandles;
            }
            
            std::vector<std::pair<std::string, SubscriptionInterval>> instrumentList(
                subscribedInstruments.begin(), subscribedInstruments.end());
            
            MarketDataRequest request = createCandlesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentList);
            
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
    
    std::vector<std::string> newInstruments;
    
    {
        std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
        
        // Add new instruments to the tracking set
        for (const auto& instrumentId : instrumentIds) {
            if (m_subscribedLastPrices.insert(instrumentId).second) {
                newInstruments.push_back(instrumentId);
            }
        }
        
        if (newInstruments.empty()) {
            std::cout << "[MarketDataStream] SubscribeLastPriceAsync: All instruments already subscribed" << std::endl;
            return;
        }
        
        std::cout << "[MarketDataStream] SubscribeLastPriceAsync: Adding " << newInstruments.size() 
                  << " new instruments, total: " << m_subscribedLastPrices.size() << std::endl;
    }
    
    if (m_running.load()) {
        // Stream already running - send subscribe request for new instruments
        try {
            MarketDataRequest request = createLastPriceRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, newInstruments);
            
            if (m_stream) {
                m_stream->Write(request);
                std::cout << "[MarketDataStream] SubscribeLastPriceAsync: Added instruments to existing stream" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeLastPriceAsync: Exception during add: " << e.what() << std::endl;
        }
        return;
    }
    
    // Start new stream with all subscribed instruments
    m_running.store(true);
    
    m_streamThread = std::thread([this, callback]() {
        try {
            // Get current subscriptions
            std::set<std::string> subscribedInstruments;
            {
                std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
                subscribedInstruments = m_subscribedLastPrices;
            }
            
            std::vector<std::string> instrumentList(subscribedInstruments.begin(), subscribedInstruments.end());
            
            MarketDataRequest request = createLastPriceRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentList);
            
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
    
    std::vector<std::string> newInstruments;
    
    {
        std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
        
        // Add new instruments to the tracking set
        for (const auto& instrumentId : instrumentIds) {
            if (m_subscribedTrades.insert(instrumentId).second) {
                newInstruments.push_back(instrumentId);
            }
        }
        
        if (newInstruments.empty()) {
            std::cout << "[MarketDataStream] SubscribeTradesAsync: All instruments already subscribed" << std::endl;
            return;
        }
        
        std::cout << "[MarketDataStream] SubscribeTradesAsync: Adding " << newInstruments.size() 
                  << " new instruments, total: " << m_subscribedTrades.size() << std::endl;
    }
    
    if (m_running.load()) {
        // Stream already running - send subscribe request for new instruments
        try {
            MarketDataRequest request = createTradesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, newInstruments);
            
            if (m_stream) {
                m_stream->Write(request);
                std::cout << "[MarketDataStream] SubscribeTradesAsync: Added instruments to existing stream" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeTradesAsync: Exception during add: " << e.what() << std::endl;
        }
        return;
    }
    
    // Start new stream with all subscribed instruments
    m_running.store(true);
    
    m_streamThread = std::thread([this, callback]() {
        try {
            // Get current subscriptions
            std::set<std::string> subscribedInstruments;
            {
                std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
                subscribedInstruments = m_subscribedTrades;
            }
            
            std::vector<std::string> instrumentList(subscribedInstruments.begin(), subscribedInstruments.end());
            
            MarketDataRequest request = createTradesRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentList);
            
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
    
    // Store depth for later use in unsubscribe
    m_orderBookDepth = depth;
    
    std::vector<std::string> newInstruments;
    
    {
        std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
        
        // Add new instruments to the tracking set
        for (const auto& instrumentId : instrumentIds) {
            if (m_subscribedOrderBooks.insert(instrumentId).second) {
                newInstruments.push_back(instrumentId);
            }
        }
        
        if (newInstruments.empty()) {
            std::cout << "[MarketDataStream] SubscribeOrderBookAsync: All instruments already subscribed" << std::endl;
            return;
        }
        
        std::cout << "[MarketDataStream] SubscribeOrderBookAsync: Adding " << newInstruments.size() 
                  << " new instruments, total: " << m_subscribedOrderBooks.size() << std::endl;
    }
    
    if (m_running.load()) {
        // Stream already running - send subscribe request for new instruments
        try {
            MarketDataRequest request = createOrderBookRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, newInstruments, depth);
            
            if (m_stream) {
                m_stream->Write(request);
                std::cout << "[MarketDataStream] SubscribeOrderBookAsync: Added instruments to existing stream" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeOrderBookAsync: Exception during add: " << e.what() << std::endl;
        }
        return;
    }
    
    // Start new stream with all subscribed instruments
    m_running.store(true);
    
    m_streamThread = std::thread([this, depth, callback]() {
        try {
            // Get current subscriptions
            std::set<std::string> subscribedInstruments;
            {
                std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
                subscribedInstruments = m_subscribedOrderBooks;
            }
            
            std::vector<std::string> instrumentList(subscribedInstruments.begin(), subscribedInstruments.end());
            
            MarketDataRequest request = createOrderBookRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentList, depth);
            
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
    
    std::vector<std::string> newInstruments;
    
    {
        std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
        
        // Add new instruments to the tracking set
        for (const auto& instrumentId : instrumentIds) {
            if (m_subscribedInfo.insert(instrumentId).second) {
                newInstruments.push_back(instrumentId);
            }
        }
        
        if (newInstruments.empty()) {
            std::cout << "[MarketDataStream] SubscribeInfoAsync: All instruments already subscribed" << std::endl;
            return;
        }
        
        std::cout << "[MarketDataStream] SubscribeInfoAsync: Adding " << newInstruments.size() 
                  << " new instruments, total: " << m_subscribedInfo.size() << std::endl;
    }
    
    if (m_running.load()) {
        // Stream already running - send subscribe request for new instruments
        try {
            MarketDataRequest request = createInfoRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, newInstruments);
            
            if (m_stream) {
                m_stream->Write(request);
                std::cout << "[MarketDataStream] SubscribeInfoAsync: Added instruments to existing stream" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[MarketDataStream] SubscribeInfoAsync: Exception during add: " << e.what() << std::endl;
        }
        return;
    }
    
    // Start new stream with all subscribed instruments
    m_running.store(true);
    
    m_streamThread = std::thread([this, callback]() {
        try {
            // Get current subscriptions
            std::set<std::string> subscribedInstruments;
            {
                std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
                subscribedInstruments = m_subscribedInfo;
            }
            
            std::vector<std::string> instrumentList(subscribedInstruments.begin(), subscribedInstruments.end());
            
            MarketDataRequest request = createInfoRequest(
                SubscriptionAction::SUBSCRIPTION_ACTION_SUBSCRIBE, instrumentList);
            
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
        // Clear all subscribed candles
        {
            std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
            m_subscribedCandles.clear();
        }
        
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
        // Clear all subscribed last prices
        {
            std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
            m_subscribedLastPrices.clear();
        }
        
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
        // Clear all subscribed trades
        {
            std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
            m_subscribedTrades.clear();
        }
        
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
        // Clear all subscribed order books
        {
            std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
            m_subscribedOrderBooks.clear();
        }
        
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
        // Clear all subscribed info
        {
            std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
            m_subscribedInfo.clear();
        }
        
        MarketDataRequest request = createInfoRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, {});
        
        if (m_stream) {
            m_stream->Write(request);
        }
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeInfoAsync: Exception: " << e.what() << std::endl;
    }
}

// ============================================================================
// Subscription Management Methods
// ============================================================================

std::set<std::string> MarketDataStream::GetSubscribedOrderBooks() const
{
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    return m_subscribedOrderBooks;
}

std::set<std::string> MarketDataStream::GetSubscribedTrades() const
{
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    return m_subscribedTrades;
}

std::set<std::pair<std::string, SubscriptionInterval>> MarketDataStream::GetSubscribedCandles() const
{
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    return m_subscribedCandles;
}

std::set<std::string> MarketDataStream::GetSubscribedInfo() const
{
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    return m_subscribedInfo;
}

std::set<std::string> MarketDataStream::GetSubscribedLastPrices() const
{
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    return m_subscribedLastPrices;
}

void MarketDataStream::UnSubscribeOrderBookAsync(const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        // If no specific instruments provided, unsubscribe all
        UnSubscribeOrderBookAsync();
        return;
    }
    
    if (!m_running.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    
    // Remove instruments from tracking set
    for (const auto& instrumentId : instrumentIds) {
        m_subscribedOrderBooks.erase(instrumentId);
    }
    
    // Send unsubscribe request for specific instruments
    try {
        MarketDataRequest request = createOrderBookRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, instrumentIds, m_orderBookDepth);
        
        if (m_stream) {
            m_stream->Write(request);
        }
        
        std::cout << "[MarketDataStream] UnSubscribeOrderBookAsync: Unsubscribed " 
                  << instrumentIds.size() << " instruments, remaining: " 
                  << m_subscribedOrderBooks.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeOrderBookAsync(vector): Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeTradesAsync(const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        UnSubscribeTradesAsync();
        return;
    }
    
    if (!m_running.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    
    for (const auto& instrumentId : instrumentIds) {
        m_subscribedTrades.erase(instrumentId);
    }
    
    try {
        MarketDataRequest request = createTradesRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, instrumentIds);
        
        if (m_stream) {
            m_stream->Write(request);
        }
        
        std::cout << "[MarketDataStream] UnSubscribeTradesAsync: Unsubscribed " 
                  << instrumentIds.size() << " instruments, remaining: " 
                  << m_subscribedTrades.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeTradesAsync(vector): Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeCandlesAsync(const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments)
{
    if (candleInstruments.empty()) {
        UnSubscribeCandlesAsync();
        return;
    }
    
    if (!m_running.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    
    for (const auto& [instrumentId, interval] : candleInstruments) {
        m_subscribedCandles.erase({instrumentId, interval});
    }
    
    try {
        MarketDataRequest request = createCandlesRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, candleInstruments);
        
        if (m_stream) {
            m_stream->Write(request);
        }
        
        std::cout << "[MarketDataStream] UnSubscribeCandlesAsync: Unsubscribed " 
                  << candleInstruments.size() << " instruments, remaining: " 
                  << m_subscribedCandles.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeCandlesAsync(vector): Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeInfoAsync(const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        UnSubscribeInfoAsync();
        return;
    }
    
    if (!m_running.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    
    for (const auto& instrumentId : instrumentIds) {
        m_subscribedInfo.erase(instrumentId);
    }
    
    try {
        MarketDataRequest request = createInfoRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, instrumentIds);
        
        if (m_stream) {
            m_stream->Write(request);
        }
        
        std::cout << "[MarketDataStream] UnSubscribeInfoAsync: Unsubscribed " 
                  << instrumentIds.size() << " instruments, remaining: " 
                  << m_subscribedInfo.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeInfoAsync(vector): Exception: " << e.what() << std::endl;
    }
}

void MarketDataStream::UnSubscribeLastPriceAsync(const std::vector<std::string>& instrumentIds)
{
    if (instrumentIds.empty()) {
        UnSubscribeLastPriceAsync();
        return;
    }
    
    if (!m_running.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(m_subscriptionsMutex);
    
    for (const auto& instrumentId : instrumentIds) {
        m_subscribedLastPrices.erase(instrumentId);
    }
    
    try {
        MarketDataRequest request = createLastPriceRequest(
            SubscriptionAction::SUBSCRIPTION_ACTION_UNSUBSCRIBE, instrumentIds);
        
        if (m_stream) {
            m_stream->Write(request);
        }
        
        std::cout << "[MarketDataStream] UnSubscribeLastPriceAsync: Unsubscribed " 
                  << instrumentIds.size() << " instruments, remaining: " 
                  << m_subscribedLastPrices.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[MarketDataStream] UnSubscribeLastPriceAsync(vector): Exception: " << e.what() << std::endl;
    }
}
