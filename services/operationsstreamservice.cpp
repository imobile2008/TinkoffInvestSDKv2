#include "operationsstreamservice.h"
#include "operationsstreamresponse.h"
#include <iostream>
#include <thread>

using grpc::ClientReader;

OperationsStream::OperationsStream(std::shared_ptr<grpc::Channel> channel, const std::string &token)
    : CustomService(token)
    , m_stub(OperationsStreamService::NewStub(channel))
    , m_channel(std::move(channel))
    , m_token(token)
    , m_streamState(0)
    , m_messageCount(0)
    , m_running(false)
{
    std::cout << "[OperationsStream] Constructor called" << std::endl;
}

OperationsStream::~OperationsStream()
{
    std::cout << "[OperationsStream] Destructor called" << std::endl;
    close();
    if (m_streamThread.joinable()) {
        m_streamThread.join();
    }
}

void OperationsStream::addAuthMetadata(grpc::ClientContext &context)
{
    std::string meta_value = "Bearer " + m_token;
    context.AddMetadata("authorization", meta_value);
    context.AddMetadata("x-app-name", APP_NAME);
}

bool OperationsStream::transitionState(int newState)
{
    int currentState = m_streamState.load();
    bool valid = false;
    
    switch (newState) {
        case kOperationsStreamConnecting: valid = (currentState == kOperationsStreamDisconnected); break;
        case kOperationsStreamConnected: valid = (currentState == kOperationsStreamConnecting); break;
        case kOperationsStreamStreaming: valid = (currentState == kOperationsStreamConnected); break;
        case kOperationsStreamClosing: valid = (currentState == kOperationsStreamStreaming || currentState == kOperationsStreamConnected || currentState == kOperationsStreamConnecting); break;
        case kOperationsStreamClosed: valid = (currentState == kOperationsStreamClosing || currentState == kOperationsStreamConnecting || currentState == kOperationsStreamDisconnected); break;
        default: break;
    }
    
    if (valid) {
        m_streamState.store(newState);
        std::cout << "[OperationsStream] State: " << currentState << " -> " << newState << std::endl;
    }
    
    return valid;
}

void OperationsStream::close()
{
    std::cout << "[OperationsStream] close() called" << std::endl;
    
    // First, signal the streaming to stop to prevent new callbacks
    bool expected = true;
    if (m_running.compare_exchange_strong(expected, false)) {
        // Cancel the gRPC context to interrupt any blocking reads
        if (m_context) {
            m_context->TryCancel();
        }
        
        // Wait for the stream thread to finish
        if (m_streamThread.joinable()) {
            m_streamThread.join();
        }
    }
    
    m_context.reset();
    transitionState(kOperationsStreamClosed);
    std::cout << "[OperationsStream] close() completed" << std::endl;
}

bool OperationsStream::isConnected() const
{
    return m_streamState.load() >= kOperationsStreamConnected;
}

uint64_t OperationsStream::getMessageCount() const
{
    return m_messageCount.load();
}

// ============================================================================
// Callback-based Async Streaming Methods
// ============================================================================

void OperationsStream::PortfolioStreamAsync(
    const std::vector<std::string>& accounts,
    CallbackFunc callback)
{
    if (accounts.empty()) {
        std::cerr << "[OperationsStream] PortfolioStreamAsync: Error - accounts is empty" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::INVALID_ARGUMENT, "accounts cannot be empty")));
        }
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[OperationsStream] PortfolioStreamAsync: Error - stream already running" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::RESOURCE_EXHAUSTED, "Stream already running - close existing stream first")));
        }
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, accounts, callback]() {
        try {
            PortfolioStreamRequest request;
            for (const auto& account : accounts) {
                if (!account.empty()) {
                    request.add_accounts(account);
                }
            }
            
            if (!transitionState(kOperationsStreamConnecting)) {
                m_running.store(false);
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            
            std::unique_ptr<ClientReader<PortfolioStreamResponse>> reader(
                m_stub->PortfolioStream(m_context.get(), request));
            
            if (!transitionState(kOperationsStreamConnected)) {
                m_running.store(false);
                return;
            }
            
            transitionState(kOperationsStreamStreaming);
            
            PortfolioStreamResponse response;
            while (reader->Read(&response)) {
                m_messageCount.fetch_add(1);
                
                // Wrap response in our handler class
                OperationsStreamResponse streamResponse(response);
                auto data = ServiceReply(streamResponse, {});
                if (callback) {
                    callback(data);
                }
            }
            
            // Finish the stream
            grpc::Status status = reader->Finish();
            
            if (!status.ok()) {
                std::cerr << "[OperationsStream] PortfolioStreamAsync: Stream finished with error: " 
                          << status.error_code() << ": " << status.error_message() << std::endl;
                if (callback) {
                    callback(ServiceReply(nullptr, status));
                }
            }
            
            transitionState(kOperationsStreamClosed);
        } catch (const std::exception& e) {
            std::cerr << "[OperationsStream] PortfolioStreamAsync: Exception: " << e.what() << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, e.what())));
            }
        } catch (...) {
            std::cerr << "[OperationsStream] PortfolioStreamAsync: Unknown exception" << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, "Unknown error in stream thread")));
            }
        }
        
        m_running.store(false);
    });
}

void OperationsStream::PositionsStreamAsync(
    const std::vector<std::string>& accounts,
    CallbackFunc callback)
{
    if (accounts.empty()) {
        std::cerr << "[OperationsStream] PositionsStreamAsync: Error - accounts is empty" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::INVALID_ARGUMENT, "accounts cannot be empty")));
        }
        return;
    }
    
    if (m_running.load()) {
        std::cerr << "[OperationsStream] PositionsStreamAsync: Error - stream already running" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::RESOURCE_EXHAUSTED, "Stream already running - close existing stream first")));
        }
        return;
    }
    
    m_running.store(true);
    
    m_streamThread = std::thread([this, accounts, callback]() {
        try {
            PositionsStreamRequest request;
            for (const auto& account : accounts) {
                if (!account.empty()) {
                    request.add_accounts(account);
                }
            }
            
            if (!transitionState(kOperationsStreamConnecting)) {
                m_running.store(false);
                return;
            }
            
            m_context = std::make_unique<grpc::ClientContext>();
            addAuthMetadata(*m_context);
            
            std::unique_ptr<ClientReader<PositionsStreamResponse>> reader(
                m_stub->PositionsStream(m_context.get(), request));
            
            if (!transitionState(kOperationsStreamConnected)) {
                m_running.store(false);
                return;
            }
            
            transitionState(kOperationsStreamStreaming);
            
            PositionsStreamResponse response;
            while (reader->Read(&response)) {
                m_messageCount.fetch_add(1);
                
                // Wrap response in our handler class
                OperationsStreamResponse streamResponse(response);
                auto data = ServiceReply(streamResponse, {});
                if (callback) {
                    callback(data);
                }
            }
            
            // Finish the stream
            grpc::Status status = reader->Finish();
            
            if (!status.ok()) {
                std::cerr << "[OperationsStream] PositionsStreamAsync: Stream finished with error: " 
                          << status.error_code() << ": " << status.error_message() << std::endl;
                if (callback) {
                    callback(ServiceReply(nullptr, status));
                }
            }
            
            transitionState(kOperationsStreamClosed);
        } catch (const std::exception& e) {
            std::cerr << "[OperationsStream] PositionsStreamAsync: Exception: " << e.what() << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, e.what())));
            }
        } catch (...) {
            std::cerr << "[OperationsStream] PositionsStreamAsync: Unknown exception" << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, "Unknown error in stream thread")));
            }
        }
        
        m_running.store(false);
    });
}

