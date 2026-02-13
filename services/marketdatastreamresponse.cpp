#include "marketdatastreamresponse.h"
#include <iostream>
#include <sstream>

MarketDataStreamResponse::PayloadType MarketDataStreamResponse::detectPayloadType() const
{
    // Check each possible payload type in the MarketDataResponse oneof field
    // The order matters - check subscription responses first (they come first)
    
    if (response_.has_subscribe_last_price_response()) {
        return PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE;
    }
    
    if (response_.has_subscribe_candles_response()) {
        return PayloadType::SUBSCRIBE_CANDLES_RESPONSE;
    }
    
    if (response_.has_subscribe_order_book_response()) {
        return PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE;
    }
    
    if (response_.has_subscribe_trades_response()) {
        return PayloadType::SUBSCRIBE_TRADES_RESPONSE;
    }
    
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
    
    // Log warning for unknown payload type - this helps diagnose "Invalid argument" errors
    std::ostringstream oss;
    oss << "[MarketDataStreamResponse] Warning: Unknown payload type detected. ";
    oss << "Debug string: " << response_.DebugString().substr(0, 200);
    std::cerr << oss.str() << std::endl;
    
    return PayloadType::UNKNOWN;
}

std::string MarketDataStreamResponse::getPayloadTypeString() const
{
    // Ensure payload type is valid before converting
    if (payload_type_ == PayloadType::UNKNOWN) {
        return "UNKNOWN";
    }
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
        case PayloadType::SUBSCRIBE_LAST_PRICE_RESPONSE:
            return "SUBSCRIBE_LAST_PRICE_RESPONSE";
        case PayloadType::SUBSCRIBE_CANDLES_RESPONSE:
            return "SUBSCRIBE_CANDLES_RESPONSE";
        case PayloadType::SUBSCRIBE_ORDER_BOOK_RESPONSE:
            return "SUBSCRIBE_ORDER_BOOK_RESPONSE";
        case PayloadType::SUBSCRIBE_TRADES_RESPONSE:
            return "SUBSCRIBE_TRADES_RESPONSE";
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
