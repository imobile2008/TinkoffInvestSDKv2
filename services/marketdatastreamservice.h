#ifndef MARKETDATASTREAMSERVICE_H
#define MARKETDATASTREAMSERVICE_H

#include <vector>
#include <set>
#include <memory>
#include <functional>
#include <atomic>
#include <mutex>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "marketdata.grpc.pb.h"
#include "customservice.h"
#include "commontypes.h"
#include "marketdatastreamresponse.h"

using grpc::Channel;
using grpc::ClientReaderWriter;

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Stream state tracking
*/
enum StreamState {
    kDisconnected = 0,
    kConnecting = 1,
    kConnected = 2,
    kStreaming = 3,
    kClosing = 4,
    kClosed = 5
};

/*!
    \brief MarketDataStream service for streaming market data
    
    This class provides market data streaming using gRPC bidirectional streaming
    with callback-based async pattern.
    Supports:
    - Candles (OHLCV data)
    - Last price updates
    - Trades
    - Order book
    - Trading status info
*/
class TINKOFFINVESTSDK_EXPORT MarketDataStream : public CustomService
{
public:
    MarketDataStream(std::shared_ptr<Channel> channel, const std::string &token);
    ~MarketDataStream();
    
    // =========================================================================
    // Callback-based Async Streaming Methods
    // =========================================================================
    
    /*!
        \brief Subscribe to candles with callback
        \param candleInstruments Vector of (instrument_id, interval) pairs
        \param callback Callback function to receive responses
    */
    void SubscribeCandlesAsync(
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to last price with callback
        \param instrumentIds Vector of instrument IDs
        \param callback Callback function to receive responses
    */
    void SubscribeLastPriceAsync(
        const std::vector<std::string>& instrumentIds,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to trades with callback
        \param instrumentIds Vector of instrument IDs
        \param callback Callback function to receive responses
    */
    void SubscribeTradesAsync(
        const std::vector<std::string>& instrumentIds,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to order book with callback
        \param instrumentIds Vector of instrument IDs
        \param depth Order book depth
        \param callback Callback function to receive responses
    */
    void SubscribeOrderBookAsync(
        const std::vector<std::string>& instrumentIds,
        int32_t depth,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to trading info with callback
        \param instrumentIds Vector of instrument IDs
        \param callback Callback function to receive responses
    */
    void SubscribeInfoAsync(
        const std::vector<std::string>& instrumentIds,
        CallbackFunc callback);
    
    /*!
        \brief Unsubscribe from candles
    */
    void UnSubscribeCandlesAsync();
    
    /*!
        \brief Unsubscribe from last price
    */
    void UnSubscribeLastPriceAsync();
    
    /*!
        \brief Unsubscribe from trades
    */
    void UnSubscribeTradesAsync();
    
    /*!
        \brief Unsubscribe from order book
    */
    void UnSubscribeOrderBookAsync();
    
    /*!
        \brief Unsubscribe from trading info
    */
    void UnSubscribeInfoAsync();
    
    // =========================================================================
    // Stream lifecycle
    // =========================================================================
    
    bool isConnected() const;
    uint64_t getMessageCount() const;
    void close();
    
    // =========================================================================
    // Subscription management
    // =========================================================================
    
    /*!
        \brief Get currently subscribed order book instruments
        \return Set of instrument IDs
    */
    std::set<std::string> GetSubscribedOrderBooks() const;
    
    /*!
        \brief Get currently subscribed trades instruments
        \return Set of instrument IDs
    */
    std::set<std::string> GetSubscribedTrades() const;
    
    /*!
        \brief Get currently subscribed candles instruments
        \return Set of (instrument_id, interval) pairs
    */
    std::set<std::pair<std::string, SubscriptionInterval>> GetSubscribedCandles() const;
    
    /*!
        \brief Get currently subscribed info instruments
        \return Set of instrument IDs
    */
    std::set<std::string> GetSubscribedInfo() const;
    
    /*!
        \brief Get currently subscribed last price instruments
        \return Set of instrument IDs
    */
    std::set<std::string> GetSubscribedLastPrices() const;
    
    /*!
        \brief Unsubscribe from specific order book instruments
        \param instrumentIds Vector of instrument IDs to unsubscribe
    */
    void UnSubscribeOrderBookAsync(const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Unsubscribe from specific trades instruments
        \param instrumentIds Vector of instrument IDs to unsubscribe
    */
    void UnSubscribeTradesAsync(const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Unsubscribe from specific candles instruments
        \param candleInstruments Vector of (instrument_id, interval) pairs to unsubscribe
    */
    void UnSubscribeCandlesAsync(const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments);
    
    /*!
        \brief Unsubscribe from specific info instruments
        \param instrumentIds Vector of instrument IDs to unsubscribe
    */
    void UnSubscribeInfoAsync(const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Unsubscribe from specific last price instruments
        \param instrumentIds Vector of instrument IDs to unsubscribe
    */
    void UnSubscribeLastPriceAsync(const std::vector<std::string>& instrumentIds);
    
private:
    std::unique_ptr<MarketDataStreamService::Stub> m_stub;
    std::shared_ptr<grpc::Channel> m_channel;
    std::string m_token;
    
    std::atomic<int> m_streamState{0};
    std::atomic<uint64_t> m_messageCount{0};
    std::atomic<bool> m_running{false};
    
    std::unique_ptr<grpc::ClientContext> m_context;
    std::shared_ptr<grpc::ClientReaderWriter<MarketDataRequest, MarketDataResponse>> m_stream;
    std::thread m_streamThread;
    
    // Subscription tracking
    mutable std::mutex m_subscriptionsMutex;
    std::set<std::string> m_subscribedOrderBooks;
    std::set<std::string> m_subscribedTrades;
    std::set<std::pair<std::string, SubscriptionInterval>> m_subscribedCandles;
    std::set<std::string> m_subscribedInfo;
    std::set<std::string> m_subscribedLastPrices;
    int32_t m_orderBookDepth = 10;
    
    void addAuthMetadata(grpc::ClientContext &context);
    bool transitionState(int newState);
    
    MarketDataRequest createCandlesRequest(SubscriptionAction action, const std::vector<std::pair<std::string, SubscriptionInterval>>& instruments);
    MarketDataRequest createOrderBookRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds, int32_t depth);
    MarketDataRequest createTradesRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
    MarketDataRequest createInfoRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
    MarketDataRequest createLastPriceRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
};

#endif // MARKETDATASTREAMSERVICE_H

