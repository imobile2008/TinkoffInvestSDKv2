#ifndef ORDERSSTREAMRESPONSE_H
#define ORDERSSTREAMRESPONSE_H

#include <string>
#include <stdexcept>
#include "orders.grpc.pb.h"

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Wrapper class for TradesStreamResponse with payload type detection
    
    This class wraps the TradesStreamResponse protobuf message and provides
    detection of the payload type. It solves the NULL reply pointer issue
    in OrdersStream callbacks by providing a consistent interface.
    
    Supported payload types:
    - ORDER_TRADES: Order trades data (main payload for orders stream)
    - UNKNOWN: No payload or unknown type
*/
class OrdersStreamResponse
{
public:
    /*!
        \brief Enumeration of possible payload types in TradesStreamResponse
    */
    enum class PayloadType {
        UNKNOWN,        ///< Unidentified or no payload
        ORDER_TRADES    ///< OrderTrades message
    };

    /*!
        \brief Default constructor creates empty response
    */
    OrdersStreamResponse() 
        : response_(), payload_type_(PayloadType::UNKNOWN) {}

    /*!
        \brief Construct from existing TradesStreamResponse
        \param response The raw TradesStreamResponse protobuf message
    */
    explicit OrdersStreamResponse(const TradesStreamResponse& response)
        : response_(response), payload_type_(detectPayloadType()) {}

    /*!
        \brief Construct from shared_ptr TradesStreamResponse
        \param responsePtr Shared pointer to TradesStreamResponse
    */
    explicit OrdersStreamResponse(const std::shared_ptr<TradesStreamResponse>& responsePtr)
        : response_(*responsePtr), payload_type_(detectPayloadType()) {}

    /*!
        \brief Copy constructor
    */
    OrdersStreamResponse(const OrdersStreamResponse& other)
        : response_(other.response_), payload_type_(other.payload_type_) {}

    /*!
        \brief Move constructor
    */
    OrdersStreamResponse(OrdersStreamResponse&& other) noexcept
        : response_(std::move(other.response_)), payload_type_(other.payload_type_) {}

    /*!
        \brief Copy assignment operator
    */
    OrdersStreamResponse& operator=(const OrdersStreamResponse& other) {
        if (this != &other) {
            response_ = other.response_;
            payload_type_ = other.payload_type_;
        }
        return *this;
    }

    /*!
        \brief Move assignment operator
    */
    OrdersStreamResponse& operator=(OrdersStreamResponse&& other) noexcept {
        if (this != &other) {
            response_ = std::move(other.response_);
            payload_type_ = other.payload_type_;
        }
        return *this;
    }

    /*!
        \brief Get the detected payload type
        \return The PayloadType enum value
    */
    PayloadType getPayloadType() const { return payload_type_; }

    /*!
        \brief Get human-readable string for payload type
        \return String representation of the payload type
    */
    std::string getPayloadTypeString() const {
        switch (payload_type_) {
            case PayloadType::ORDER_TRADES: return "ORDER_TRADES";
            case PayloadType::UNKNOWN: 
            default: return "UNKNOWN";
        }
    }

    /*!
        \brief Check if a specific payload type is present
        \param type The PayloadType to check for
        \return true if the specified payload type is present
    */
    bool isPayloadType(PayloadType type) const { return payload_type_ == type; }

    /*!
        \brief Get the raw TradesStreamResponse
        \return Const reference to the underlying TradesStreamResponse
    */
    const TradesStreamResponse& getRawResponse() const { return response_; }

    /*!
        \brief Get mutable access to raw TradesStreamResponse
        \return Reference to the underlying TradesStreamResponse
    */
    TradesStreamResponse& getMutableRawResponse() { return response_; }

    /*!
        \brief Get OrderTrades payload (type-safe accessor)
        \return Const reference to OrderTrades message
        \throws std::runtime_error if payload type is not ORDER_TRADES
    */
    const OrderTrades& getOrderTrades() const {
        validatePayloadType(PayloadType::ORDER_TRADES);
        return response_.order_trades();
    }

    /*!
        \brief Check if this is an OrderTrades payload
        \return true if the payload type is ORDER_TRADES
    */
    bool hasOrderTrades() const {
        return payload_type_ == PayloadType::ORDER_TRADES;
    }

    /*!
        \brief Check if this response has any valid payload
        \return true if there's any payload present
    */
    bool hasPayload() const {
        return payload_type_ != PayloadType::UNKNOWN;
    }

    /*!
        \brief Get debug string representation
        \return Debug string of the underlying TradesStreamResponse
    */
    std::string debugString() const {
        return response_.DebugString();
    }

    /*!
        \brief Static method to get string name for payload type
        \param type The PayloadType to get name for
        \return String name of the payload type
    */
    static std::string payloadTypeToString(PayloadType type) {
        switch (type) {
            case PayloadType::ORDER_TRADES: return "ORDER_TRADES";
            case PayloadType::UNKNOWN: 
            default: return "UNKNOWN";
        }
    }

private:
    TradesStreamResponse response_;
    PayloadType payload_type_;

    /*!
        \brief Detect which payload type is set in the TradesStreamResponse
        \return The detected PayloadType enum value
    */
    PayloadType detectPayloadType() const {
        if (response_.has_order_trades()) {
            return PayloadType::ORDER_TRADES;
        }
        return PayloadType::UNKNOWN;
    }

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
    \brief Streaming callback function type that uses OrdersStreamResponse
    
    This is the recommended callback type for orders stream that provides
    automatic payload type detection and type-safe access.
    
    \param response The OrdersStreamResponse wrapper with detected payload type
*/
using OrdersStreamCallback = std::function<void(OrdersStreamResponse)>;

#endif // ORDERSSTREAMRESPONSE_H

