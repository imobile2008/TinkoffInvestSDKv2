#ifndef ORDERSSTREAMSERVICE_H
#define ORDERSSTREAMSERVICE_H

#include <thread>
#include <set>
#include <atomic>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "customservice.h"
#include "orders.grpc.pb.h"
#include "commontypes.h"
#include "marketdatastreamcoroutine.h"

using grpc::ClientAsyncReader;
using grpc::Channel;

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief  Сервис торговых поручений в режиме стриминга

    Сервис предназначен для получения потока сделок пользователя.
*/
class TINKOFFINVESTSDK_EXPORT OrdersStream: public CustomService
{

public:
    OrdersStream(std::shared_ptr<Channel> channel, const std::string &token);
    ~OrdersStream();

    /// Поток сделок пользователя, блокирующий вызов
    void TradesStream(const Strings &accounts, CallbackFunc callback);
    /// Поток сделок пользователя, асинхронный вызов
    void TradesStreamAsync(const Strings &accounts, CallbackFunc callback);
    /// Закрыть поток
    void close();
    
    // =========================================================================
    // Coroutine-based subscription methods (C++20)
    // =========================================================================
    
    /*!
        \brief Subscribe to trades stream using C++20 coroutines
        \param accounts Vector of account IDs
        \return MarketDataStreamGenerator that yields TradesStreamResponse
    */
    MarketDataStreamGenerator<TradesStreamResponse> TradesStreamCoroutine(const Strings &accounts);
    
private:
    void onStreamFinished();
    
    std::atomic<bool> m_running;
    std::thread m_streamThread;
    std::unique_ptr<grpc::ClientContext> m_context;
    std::unique_ptr<OrdersStreamService::Stub> m_ordersStreamService;

};

#endif // ORDERSSTREAMSERVICE_H
