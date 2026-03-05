#ifndef OPERATIONSSTREAMRESPONSE_H
#define OPERATIONSSTREAMRESPONSE_H

#include <memory>
#include <string>
#include <vector>
#include "operations.pb.h"

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Wrapper for OperationsStream responses
    
    This class provides a unified interface for handling different types
    of OperationsStream responses (PortfolioStream and PositionsStream).
*/
class OperationsStreamResponse
{
public:
    OperationsStreamResponse() = default;
    explicit OperationsStreamResponse(const PortfolioStreamResponse& response);
    explicit OperationsStreamResponse(const PositionsStreamResponse& response);
    
    // Check what type of payload is available
    bool hasPortfolio() const;
    bool hasPositions() const;
    bool hasSubscriptions() const;
    bool hasPing() const;
    
    // Get the portfolio response
    const PortfolioStreamResponse& getPortfolio() const;
    
    // Get the positions response
    const PositionsStreamResponse& getPositions() const;
    
    // Get subscription result
    const PortfolioSubscriptionResult* getPortfolioSubscription() const;
    const PositionsSubscriptionResult* getPositionsSubscription() const;
    
    // Get ping
    const Ping* getPing() const;
    
    // Debug string
    std::string debugString() const;
    
private:
    enum class ResponseType {
        None,
        Portfolio,
        Positions
    };
    
    ResponseType m_type{ResponseType::None};
    PortfolioStreamResponse m_portfolioResponse;
    PositionsStreamResponse m_positionsResponse;
};

#endif // OPERATIONSSTREAMRESPONSE_H

