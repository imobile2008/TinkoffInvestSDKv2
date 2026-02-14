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
#include "marketdatastreamcoroutine.h"

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
    with C++20 coroutines.
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
    // C++20 Coroutine-based Streaming Methods
    // =========================================================================
    
    /*!
        \brief Subscribe to candles using C++20 coroutines
        \param candleInstruments Vector of (instrument_id, interval) pairs
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeCandlesCoroutine(
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments);
    
    /*!
        \brief Subscribe to last price using C++20 coroutines
        \param instrumentIds Vector of instrument IDs
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeLastPriceCoroutine(
        const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Subscribe to trades using C++20 coroutines
        \param instrumentIds Vector of instrument IDs
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeTradesCoroutine(
        const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Subscribe to order book using C++20 coroutines
        \param instrumentIds Vector of instrument IDs
        \param depth Order book depth
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeOrderBookCoroutine(
        const std::vector<std::string>& instrumentIds, int32_t depth = 10);
    
    /*!
        \brief Subscribe to trading info using C++20 coroutines
        \param instrumentIds Vector of instrument IDs
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeInfoCoroutine(
        const std::vector<std::string>& instrumentIds);
    
    /*!
        \brief Subscribe to all market data types using C++20 coroutines
        \param candleInstruments Vector of (instrument_id, interval) pairs for candles
        \param orderBookInstruments Vector of instrument IDs for order book
        \param orderBookDepth Order book depth
        \param tradesInstruments Vector of instrument IDs for trades
        \param infoInstruments Vector of instrument IDs for trading info
        \param lastPriceInstruments Vector of instrument IDs for last price
        \return MarketDataStreamGenerator that yields MarketDataStreamResponse
    */
    MarketDataStreamGenerator<MarketDataStreamResponse> SubscribeAllCoroutine(
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
        const std::vector<std::string>& orderBookInstruments,
        int32_t orderBookDepth,
        const std::vector<std::string>& tradesInstruments,
        const std::vector<std::string>& infoInstruments,
        const std::vector<std::string>& lastPriceInstruments);
    
    // =========================================================================
    // Callback-based Async Streaming Methods (non-coroutine)
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
    
    void addAuthMetadata(grpc::ClientContext &context);
    bool transitionState(int newState);
    
    MarketDataRequest createCandlesRequest(SubscriptionAction action, const std::vector<std::pair<std::string, SubscriptionInterval>>& instruments);
    MarketDataRequest createOrderBookRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds, int32_t depth);
    MarketDataRequest createTradesRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
    MarketDataRequest createInfoRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
    MarketDataRequest createLastPriceRequest(SubscriptionAction action, const std::vector<std::string>& instrumentIds);
};

#endif // MARKETDATASTREAMSERVICE_H

