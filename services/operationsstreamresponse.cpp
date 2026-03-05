#include "operationsstreamresponse.h"

OperationsStreamResponse::OperationsStreamResponse(const PortfolioStreamResponse& response)
    : m_type(ResponseType::Portfolio)
    , m_portfolioResponse(response)
{
}

OperationsStreamResponse::OperationsStreamResponse(const PositionsStreamResponse& response)
    : m_type(ResponseType::Positions)
    , m_positionsResponse(response)
{
}

bool OperationsStreamResponse::hasPortfolio() const
{
    return m_type == ResponseType::Portfolio && m_portfolioResponse.has_portfolio();
}

bool OperationsStreamResponse::hasPositions() const
{
    return m_type == ResponseType::Positions && m_positionsResponse.has_position();
}

bool OperationsStreamResponse::hasSubscriptions() const
{
    if (m_type == ResponseType::Portfolio) {
        return m_portfolioResponse.has_subscriptions();
    }
    if (m_type == ResponseType::Positions) {
        return m_positionsResponse.has_subscriptions();
    }
    return false;
}

bool OperationsStreamResponse::hasPing() const
{
    if (m_type == ResponseType::Portfolio) {
        return m_portfolioResponse.has_ping();
    }
    if (m_type == ResponseType::Positions) {
        return m_positionsResponse.has_ping();
    }
    return false;
}

const PortfolioStreamResponse& OperationsStreamResponse::getPortfolio() const
{
    return m_portfolioResponse;
}

const PositionsStreamResponse& OperationsStreamResponse::getPositions() const
{
    return m_positionsResponse;
}

const PortfolioSubscriptionResult* OperationsStreamResponse::getPortfolioSubscription() const
{
    if (m_type == ResponseType::Portfolio && m_portfolioResponse.has_subscriptions()) {
        return &m_portfolioResponse.subscriptions();
    }
    return nullptr;
}

const PositionsSubscriptionResult* OperationsStreamResponse::getPositionsSubscription() const
{
    if (m_type == ResponseType::Positions && m_positionsResponse.has_subscriptions()) {
        return &m_positionsResponse.subscriptions();
    }
    return nullptr;
}

const Ping* OperationsStreamResponse::getPing() const
{
    if (m_type == ResponseType::Portfolio && m_portfolioResponse.has_ping()) {
        return &m_portfolioResponse.ping();
    }
    if (m_type == ResponseType::Positions && m_positionsResponse.has_ping()) {
        return &m_positionsResponse.ping();
    }
    return nullptr;
}

std::string OperationsStreamResponse::debugString() const
{
    switch (m_type) {
        case ResponseType::Portfolio:
            return m_portfolioResponse.DebugString();
        case ResponseType::Positions:
            return m_positionsResponse.DebugString();
        default:
            return "Empty OperationsStreamResponse";
    }
}

