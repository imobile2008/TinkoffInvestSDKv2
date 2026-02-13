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
    
    This class provides market data streaming using gRPC bidirectional streaming.
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
    // Async subscription methods
    // =========================================================================
    
    /*!
        \brief Subscribe to candles asynchronously
        \param candleInstruments Vector of (instrument_id, interval) pairs
        \param callback Function to call for each response
    */
    void SubscribeCandles(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    
    /*!
        \brief Subscribe to last price asynchronously
        \param instrumentIds Vector of instrument IDs
        \param callback Function to call for each response
    */
    void SubscribeLastPrice(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    
    /*!
        \brief Subscribe to trades asynchronously
        \param instrumentIds Vector of instrument IDs
        \param callback Function to call for each response
    */
    void SubscribeTrades(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    
    /*!
        \brief Subscribe to order book asynchronously
        \param instrumentIds Vector of instrument IDs
        \param depth Order book depth
        \param callback Function to call for each response
    */
    void SubscribeOrderBook(const std::vector<std::string> &instrumentIds, int32_t depth, CallbackFunc callback);
    
    /*!
        \brief Subscribe to trading info asynchronously
        \param instrumentIds Vector of instrument IDs
        \param callback Function to call for each response
    */
    void SubscribeInfo(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    
    // =========================================================================
    // Combined subscription methods (single stream for all types)
    // =========================================================================
    
    /*!
        \brief Subscribe to all market data types in a single stream
        \param candleInstruments Vector of (instrument_id, interval) pairs for candles
        \param orderBookInstruments Vector of instrument IDs for order book
        \param orderBookDepth Order book depth
        \param tradesInstruments Vector of instrument IDs for trades
        \param infoInstruments Vector of instrument IDs for trading info
        \param lastPriceInstruments Vector of instrument IDs for last price
        \param callback Function to call for each response
    */
    void SubscribeAll(
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
        const std::vector<std::string>& orderBookInstruments,
        int32_t orderBookDepth,
        const std::vector<std::string>& tradesInstruments,
        const std::vector<std::string>& infoInstruments,
        const std::vector<std::string>& lastPriceInstruments,
        CallbackFunc callback);
    
    /*!
        \brief Subscribe to all market data types asynchronously (run in background thread)
    */
    void SubscribeAllAsync(
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
        const std::vector<std::string>& orderBookInstruments,
        int32_t orderBookDepth,
        const std::vector<std::string>& tradesInstruments,
        const std::vector<std::string>& infoInstruments,
        const std::vector<std::string>& lastPriceInstruments,
        CallbackFunc callback);
    
    /*!
        \brief Unsubscribe from all market data types
    */
    void UnSubscribeAll();
    
    /*!
        \brief Unsubscribe from all market data types asynchronously
    */
    void UnSubscribeAllAsync();
    
    // =========================================================================
    // Async methods (run in background thread)
    // =========================================================================
    
    void SubscribeCandlesAsync(const std::vector<std::pair<std::string, SubscriptionInterval>> &candleInstruments, CallbackFunc callback);
    void SubscribeOrderBookAsync(const std::vector<std::string> &instrumentIds, int32_t depth, CallbackFunc callback);
    void SubscribeTradesAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    void SubscribeInfoAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    void SubscribeLastPriceAsync(const std::vector<std::string> &instrumentIds, CallbackFunc callback);
    
    // =========================================================================
    // Unsubscription methods
    // =========================================================================
    
    void UnSubscribeCandles();
    void UnSubscribeOrderBook();
    void UnSubscribeTrades();
    void UnSubscribeLastPrice();
    void UnSubscribeInfo();
    
    void UnSubscribeCandlesAsync();
    void UnSubscribeOrderBookAsync();
    void UnSubscribeTradesAsync();
    void UnSubscribeLastPriceAsync();
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
    MarketDataRequest createCombinedRequest(
        SubscriptionAction action,
        const std::vector<std::pair<std::string, SubscriptionInterval>>& candleInstruments,
        const std::vector<std::string>& orderBookInstruments,
        int32_t orderBookDepth,
        const std::vector<std::string>& tradesInstruments,
        const std::vector<std::string>& infoInstruments,
        const std::vector<std::string>& lastPriceInstruments);
    
    // Internal streaming implementation
    template<typename RequestType>
    void streamLoop(const MarketDataRequest& request, CallbackFunc callback);
};

#endif // MARKETDATASTREAMSERVICE_H

