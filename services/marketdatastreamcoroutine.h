/**
 * @file marketdatastreamcoroutine.h
 * @brief Streaming utilities for MarketDataStream
 * 
 * This file provides streaming utilities for market data streaming.
 * Uses callback-based approach for maximum compatibility across compilers.
 */

#ifndef MARKETDATASTREAMCOROUTINE_H
#define MARKETDATASTREAMCOROUTINE_H

#include <optional>
#include <atomic>
#include <memory>
#include <functional>
#include <iostream>
#include "marketdatastreamresponse.h"

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Result type for gRPC operations
*/
struct GrpcResult {
    bool ok;
    std::string error_message;
    
    static GrpcResult success() { return {true, ""}; }
    static GrpcResult failure(const std::string& msg) { return {false, msg}; }
};

/*!
    \brief Streaming callback function type that uses MarketDataStreamResponse
    
    This is the recommended callback type for market data streaming that provides
    automatic payload type detection and type-safe access.
    
    \param response The MarketDataStreamResponse wrapper with detected payload type
*/
using MarketDataStreamCallback = std::function<void(MarketDataStreamResponse)>;

#endif // MARKETDATASTREAMCOROUTINE_H

