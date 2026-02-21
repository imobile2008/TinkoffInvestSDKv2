#include "ordersstreamservice.h"
#include "ordersstreamresponse.h"
#include <iostream>
#include <thread>

using grpc::ClientReader;

OrdersStream::OrdersStream(std::shared_ptr<grpc::Channel> channel, const std::string &token) :
    CustomService(token),
    m_ordersStreamService(OrdersStreamService::NewStub(channel)),
    m_running(false)
{    

}

OrdersStream::~OrdersStream()
{
    close();
    
    // Wait for the stream thread to finish
    if (m_streamThread.joinable()) {
        m_streamThread.join();
    }
}

void OrdersStream::close()
{
    // Signal the stream to stop
    bool expected = true;
    if (m_running.compare_exchange_strong(expected, false)) {
        // Cancel the context to interrupt any blocking reads
        if (m_context) {
            m_context->TryCancel();
        }
        
        // Wait for the stream thread to finish
        if (m_streamThread.joinable()) {
            m_streamThread.join();
        }
    }
    
    m_context.reset();
}

void OrdersStream::onStreamFinished()
{
    m_running.store(false);
}

void OrdersStream::TradesStreamAsync(const Strings &accounts, CallbackFunc callback)
{
    // Prevent multiple concurrent subscriptions
    if (m_running.load()) {
        std::cerr << "[OrdersStream] TradesStreamAsync: Error - stream already running" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::RESOURCE_EXHAUSTED, "Stream already running - close existing stream first")));
        }
        return;
    }
    
    // Validate input
    if (accounts.empty()) {
        std::cerr << "[OrdersStream] TradesStreamAsync: Error - accounts is empty" << std::endl;
        if (callback) {
            callback(ServiceReply(nullptr, grpc::Status(grpc::INVALID_ARGUMENT, "accounts cannot be empty")));
        }
        return;
    }
    
    m_running.store(true);
    
    // Use a simple thread with blocking reads, similar to MarketDataStream
    m_streamThread = std::thread([this, accounts, callback]() mutable {
        try {
            TradesStreamRequest request;
            for (auto &account : accounts) {
                if (!account.empty())
                    request.add_accounts(account);
            }
            
            // Create context
            m_context = std::make_unique<grpc::ClientContext>();
            std::string meta_value = "Bearer " + m_token;
            m_context->AddMetadata("authorization", meta_value);
            m_context->AddMetadata("x-app-name", APP_NAME);
            
            // Create and start the stream
            std::unique_ptr<ClientReader<TradesStreamResponse>> reader(
                m_ordersStreamService->TradesStream(m_context.get(), request));
            
            TradesStreamResponse response;
            while (reader->Read(&response)) {
                try {
                    // Use OrdersStreamResponse wrapper for proper callback handling
                    OrdersStreamResponse streamResponse(response);
                    auto data = ServiceReply(streamResponse, {});
                    if (callback) {
                        callback(data);
                    }
                } catch (const std::exception& e) {
                    std::cerr << "[OrdersStream] Error processing response: " << e.what() << std::endl;
                }
            }
            
            // Finish the stream
            grpc::Status status = reader->Finish();
            
        } catch (const std::exception& e) {
            std::cerr << "[OrdersStream] TradesStreamAsync: Exception in stream thread: " << e.what() << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, e.what())));
            }
        } catch (...) {
            std::cerr << "[OrdersStream] TradesStreamAsync: Unknown exception in stream thread" << std::endl;
            if (callback) {
                callback(ServiceReply(nullptr, grpc::Status(grpc::UNKNOWN, "Unknown error in stream thread")));
            }
        }
        
        m_running.store(false);
    });
}

void OrdersStream::TradesStream(const Strings &accounts, CallbackFunc callback)
{
    TradesStreamRequest request;
    for (auto &account: accounts)
    {
        if (!account.empty())
            request.add_accounts(account);
    }
    TradesStreamResponse reply;
    ClientContext context;
    std::string meta_value = "Bearer " + m_token;
    context.AddMetadata("authorization", meta_value);
    context.AddMetadata("x-app-name", APP_NAME);

    std::unique_ptr<ClientReader<TradesStreamResponse> > reader(
        m_ordersStreamService->TradesStream(&context, request));
    while (reader->Read(&reply)) {
        // Use OrdersStreamResponse wrapper for proper callback handling
        OrdersStreamResponse streamResponse(reply);
        auto data = ServiceReply(streamResponse, {});
        if (callback) callback(data);
    }
    Status status = reader->Finish();
    if (!status.ok()) {
        std::cout << "TradesStream rpc failed." << std::endl;
    }
}
