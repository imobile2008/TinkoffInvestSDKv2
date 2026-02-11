#include "marketdatastreamresponse.h"

MarketDataStreamResponse::PayloadType MarketDataStreamResponse::detectPayloadType() const
{
    // Check each possible payload type in the MarketDataResponse oneof field
    // The order matters - check most common types first for performance
    
    if (response_.has_trading_status()) {
        return PayloadType::TRADING_STATUS;
    }
    
    if (response_.has_subscribe_info_response()) {
        return PayloadType::SUBSCRIBE_INFO_RESPONSE;
    }
    
    if (response_.has_candle()) {
        return PayloadType::CANDLE;
    }
    
    if (response_.has_orderbook()) {
        return PayloadType::ORDERBOOK;
    }
    
    if (response_.has_trade()) {
        return PayloadType::TRADE;
    }
    
    if (response_.has_last_price()) {
        return PayloadType::LAST_PRICE;
    }
    
    if (response_.has_ping()) {
        return PayloadType::PING;
    }
    
    return PayloadType::UNKNOWN;
}

std::string MarketDataStreamResponse::getPayloadTypeString() const
{
    return payloadTypeToString(payload_type_);
}

std::string MarketDataStreamResponse::payloadTypeToString(MarketDataStreamResponse::PayloadType type)
{
    switch (type) {
        case PayloadType::UNKNOWN:
            return "UNKNOWN";
        case PayloadType::TRADING_STATUS:
            return "TRADING_STATUS";
        case PayloadType::SUBSCRIBE_INFO_RESPONSE:
            return "SUBSCRIBE_INFO_RESPONSE";
        case PayloadType::CANDLE:
            return "CANDLE";
        case PayloadType::ORDERBOOK:
            return "ORDERBOOK";
        case PayloadType::TRADE:
            return "TRADE";
        case PayloadType::LAST_PRICE:
            return "LAST_PRICE";
        case PayloadType::PING:
            return "PING";
        default:
            return "UNRECOGNIZED";
    }
}
