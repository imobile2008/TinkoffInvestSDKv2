#ifndef MARKETDATASTREAMRESPONSE_H
#define MARKETDATASTREAMRESPONSE_H

#include <string>
#include <stdexcept>
#include "marketdata.grpc.pb.h"

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Wrapper class for MarketDataResponse with automatic payload type detection
    
    This class wraps the raw MarketDataResponse protobuf message and provides
    automatic detection and type-safe access to different payload types in the
    MarketDataResponse oneof field. It solves the "Received unknown payload type"
    issue by properly identifying which payload type is present.
    
    Supported payload types:
    - TRADING_STATUS: Real-time trading status updates
    - SUBSCRIBE_INFO_RESPONSE: Subscription confirmation and info responses  
    - CANDLE: OHLCV candle data
    - ORDERBOOK: Order book/bid-ask data
    - TRADE: Trade/tick data
    - LAST_PRICE: Last price updates
    - PING: Keep-alive ping messages
*/
class MarketDataStreamResponse
{
public:
    /*!
        \brief Enumeration of all possible payload types in MarketDataResponse
    */
    enum class PayloadType {
        UNKNOWN,           ///< Unidentified or no payload
        TRADING_STATUS,    ///< TradingStatus message
        SUBSCRIBE_INFO_RESPONSE, ///< SubscribeInfoResponse message
        SUBSCRIBE_LAST_PRICE_RESPONSE, ///< SubscribeLastPriceResponse message
        SUBSCRIBE_CANDLES_RESPONSE, ///< SubscribeCandlesResponse message
        SUBSCRIBE_ORDER_BOOK_RESPONSE, ///< SubscribeOrderBookResponse message
        SUBSCRIBE_TRADES_RESPONSE, ///< SubscribeTradesResponse message
        CANDLE,            ///< Candle message
        ORDERBOOK,         ///< OrderBook message
        TRADE,             ///< Trade message  
        LAST_PRICE,        ///< LastPrice message
        PING               ///< Ping message
    };

    /*!
        \brief Default constructor creates empty response
    */
    MarketDataStreamResponse() 
        : response_(), payload_type_(PayloadType::UNKNOWN) {}

    /*!
        \brief Construct from existing MarketDataResponse
        \param response The raw MarketDataResponse protobuf message
    */
    explicit MarketDataStreamResponse(const MarketDataResponse& response)
        : response_(response), payload_type_(detectPayloadType()) {}

    /*!
        \brief Construct from shared_ptr MarketDataResponse
        \param responsePtr Shared pointer to MarketDataResponse
    */
    explicit MarketDataStreamResponse(const std::shared_ptr<MarketDataResponse>& responsePtr)
        : response_(*responsePtr), payload_type_(detectPayloadType()) {}

    /*!
        \brief Copy constructor
    */
    MarketDataStreamResponse(const MarketDataStreamResponse& other)
        : response_(other.response_), payload_type_(other.payload_type_) {}

    /*!
        \brief Move constructor
    */
    MarketDataStreamResponse(MarketDataStreamResponse&& other) noexcept
        : response_(std::move(other.response_)), payload_type_(other.payload_type_) {}

    /*!
        \brief Copy assignment operator
    */
    MarketDataStreamResponse& operator=(const MarketDataStreamResponse& other) {
        if (this != &other) {
            response_ = other.response_;
            payload_type_ = other.payload_type_;
        }
        return *this;
    }

    /*!
        \brief Move assignment operator
    */
    MarketDataStreamResponse& operator=(MarketDataStreamResponse&& other) noexcept {
        if (this != &other) {
            response_ = std::move(other.response_);
            payload_type_ = other.payload_type_;
        }
        return *this;
    }

    /*!
        \brief Get the detected payload type
        \return The PayloadType enum value identifying which payload is present
    */
    PayloadType getPayloadType() const { return payload_type_; }

    /*!
        \brief Get human-readable string for payload type
        \return String representation of the payload type
    */
    std::string getPayloadTypeString() const;

    /*!
        \brief Check if a specific payload type is present
        \param type The PayloadType to check for
        \return true if the specified payload type is present
    */
    bool isPayloadType(PayloadType type) const { return payload_type_ == type; }

    /*!
        \brief Get the raw MarketDataResponse
        \return Const reference to the underlying MarketDataResponse
    */
    const MarketDataResponse& getRawResponse() const { return response_; }

    /*!
        \brief Get mutable access to raw MarketDataResponse
        \return Reference to the underlying MarketDataResponse
    */
    MarketDataResponse& getMutableRawResponse() { return response_; }

    /*!
        \brief Get SubscribeLastPriceResponse payload (type-safe accessor)
        \return Const reference to SubscribeLastPriceResponse message
        \throws std::runtime_error if payload type is not SUBSCRIBE_LAST_PRICE_RESPONSE
    */
    const SubscribeLastPriceResponse& getSubscribeLastPriceResponse() const {
        validatePayloadType(PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE);
        return response_.subscribe_last_price_response();
    }

    /*!
        \brief Get SubscribeCandlesResponse payload (type-safe accessor)
        \return Const reference to SubscribeCandlesResponse message
        \throws std::runtime_error if payload type is not SUBSCRIBE_CANDLES_RESPONSE
    */
    const SubscribeCandlesResponse& getSubscribeCandlesResponse() const {
        validatePayloadType(PayloadType::SUBSCRIBE_CANDLES_RESPONSE);
        return response_.subscribe_candles_response();
    }

    /*!
        \brief Get SubscribeOrderBookResponse payload (type-safe accessor)
        \return Const reference to SubscribeOrderBookResponse message
        \throws std::runtime_error if payload type is not SUBSCRIBE_ORDER_BOOK_RESPONSE
    */
    const SubscribeOrderBookResponse& getSubscribeOrderBookResponse() const {
        validatePayloadType(PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE);
        return response_.subscribe_order_book_response();
    }

    /*!
        \brief Get SubscribeTradesResponse payload (type-safe accessor)
        \return Const reference to SubscribeTradesResponse message
        \throws std::runtime_error if payload type is not SUBSCRIBE_TRADES_RESPONSE
    */
    const SubscribeTradesResponse& getSubscribeTradesResponse() const {
        validatePayloadType(PayloadType::SUBSCRIBE_TRADES_RESPONSE);
        return response_.subscribe_trades_response();
    }

    /*!
        \brief Get TradingStatus payload (type-safe accessor)
        \return Const reference to TradingStatus message
        \throws std::runtime_error if payload type is not TRADING_STATUS
    */
    const TradingStatus& getTradingStatus() const {
        validatePayloadType(PayloadType::TRADING_STATUS);
        return response_.trading_status();
    }

    /*!
        \brief Get SubscribeInfoResponse payload (type-safe accessor)
        \return Const reference to SubscribeInfoResponse message
        \throws std::runtime_error if payload type is not SUBSCRIBE_INFO_RESPONSE
    */
    const SubscribeInfoResponse& getSubscribeInfoResponse() const {
        validatePayloadType(PayloadType::SUBSCRIBE_INFO_RESPONSE);
        return response_.subscribe_info_response();
    }

    /*!
        \brief Get Candle payload (type-safe accessor)
        \return Const reference to Candle message
        \throws std::runtime_error if payload type is not CANDLE
    */
    const Candle& getCandle() const {
        validatePayloadType(PayloadType::CANDLE);
        return response_.candle();
    }

    /*!
        \brief Get OrderBook payload (type-safe accessor)
        \return Const reference to OrderBook message
        \throws std::runtime_error if payload type is not ORDERBOOK
    */
    const OrderBook& getOrderBook() const {
        validatePayloadType(PayloadType::ORDERBOOK);
        return response_.orderbook();
    }

    /*!
        \brief Get Trade payload (type-safe accessor)
        \return Const reference to Trade message
        \throws std::runtime_error if payload type is not TRADE
    */
    const Trade& getTrade() const {
        validatePayloadType(PayloadType::TRADE);
        return response_.trade();
    }

    /*!
        \brief Get LastPrice payload (type-safe accessor)
        \return Const reference to LastPrice message
        \throws std::runtime_error if payload type is not LAST_PRICE
    */
    const LastPrice& getLastPrice() const {
        validatePayloadType(PayloadType::LAST_PRICE);
        return response_.last_price();
    }

    /*!
        \brief Get Ping payload (type-safe accessor)
        \return Const reference to Ping message
        \throws std::runtime_error if payload type is not PING
    */
    const Ping& getPing() const {
        validatePayloadType(PayloadType::PING);
        return response_.ping();
    }

    /*!
        \brief Check if this is a LastPrice payload
        \return true if the payload type is LAST_PRICE
    */
    bool hasLastPrice() const {
        return payload_type_ == PayloadType::LAST_PRICE;
    }

    /*!
        \brief Get the tracking_id from SubscribeInfoResponse
        \return String containing the tracking ID, empty if not available
    */
    std::string getTrackingId() const {
        if (isPayloadType(PayloadType::SUBSCRIBE_INFO_RESPONSE)) {
            return response_.subscribe_info_response().tracking_id();
        }
        return "";
    }

    /*!
        \brief Check if this is a subscription confirmation message
        \return true if this is a SubscribeInfoResponse with subscription confirmation
    */
    bool isSubscriptionConfirmation() const {
        return isPayloadType(PayloadType::SUBSCRIBE_INFO_RESPONSE);
    }

    /*!
        \brief Check if this is any subscription response (candles, trades, orderbook, lastprice, info)
        \return true if this is any type of subscription response
    */
    bool isSubscriptionResponse() const {
        return isPayloadType(PayloadType::SUBSCRIBE_INFO_RESPONSE) ||
               isPayloadType(PayloadType::SUBSCRIBE_CANDLES_RESPONSE) ||
               isPayloadType(PayloadType::SUBSCRIBE_TRADES_RESPONSE) ||
               isPayloadType(PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE) ||
               isPayloadType(PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE);
    }

    /*!
        \brief Get the subscription status from SubscribeInfoResponse
        \return The subscription status, or empty string if not available
    */
    std::string getSubscriptionStatus() const {
        if (isPayloadType(PayloadType::SUBSCRIBE_INFO_RESPONSE)) {
            // SubscribeInfoResponse contains tracking_id, status is in the subscription status field
            // For now, we return the tracking_id as confirmation indicator
            return response_.subscribe_info_response().tracking_id();
        }
        return "";
    }

    /*!
        \brief Check if this is a SubscribeCandlesResponse
        \return true if payload type is SUBSCRIBE_CANDLES_RESPONSE
    */
    bool isSubscribeCandlesResponse() const {
        return isPayloadType(PayloadType::SUBSCRIBE_CANDLES_RESPONSE);
    }

    /*!
        \brief Check if this is a SubscribeTradesResponse
        \return true if payload type is SUBSCRIBE_TRADES_RESPONSE
    */
    bool isSubscribeTradesResponse() const {
        return isPayloadType(PayloadType::SUBSCRIBE_TRADES_RESPONSE);
    }

    /*!
        \brief Check if this is a SubscribeOrderBookResponse
        \return true if payload type is SUBSCRIBE_ORDER_BOOK_RESPONSE
    */
    bool isSubscribeOrderBookResponse() const {
        return isPayloadType(PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE);
    }

    /*!
        \brief Check if this is a SubscribeLastPriceResponse
        \return true if payload type is SUBSCRIBE_LAST_PRICE_RESPONSE
    */
    bool isSubscribeLastPriceResponse() const {
        return isPayloadType(PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE);
    }

    /*!
        \brief Get debug string representation
        \return Debug string of the underlying MarketDataResponse
    */
    std::string debugString() const {
        return response_.DebugString();
    }

    /*!
        \brief Static method to get string name for payload type
        \param type The PayloadType to get name for
        \return String name of the payload type
    */
    static std::string payloadTypeToString(PayloadType type);

private:
    MarketDataResponse response_;
    PayloadType payload_type_;

    /*!
        \brief Detect which payload type is set in the MarketDataResponse
        \return The detected PayloadType enum value
    */
    PayloadType detectPayloadType() const;

    /*!
        \brief Validate that the current payload type matches expected type
        \param expectedType The expected PayloadType
        \throws std::runtime_error if types don't match
    */
    void validatePayloadType(PayloadType expectedType) const {
        if (payload_type_ != expectedType) {
            throw std::runtime_error(
                "Invalid payload type access. Expected: " + 
                payloadTypeToString(expectedType) + 
                ", Actual: " + 
                payloadTypeToString(payload_type_)
            );
        }
    }
};

/*!
    \brief Streaming callback function type that uses MarketDataStreamResponse
    
    This is the recommended callback type for market data streaming that provides
    automatic payload type detection and type-safe access.
    
    \param response The MarketDataStreamResponse wrapper with detected payload type
*/
using MarketDataStreamCallback = std::function<void(MarketDataStreamResponse)>;

#endif // MARKETDATASTREAMRESPONSE_H

