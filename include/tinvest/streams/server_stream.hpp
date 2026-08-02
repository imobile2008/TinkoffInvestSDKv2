#pragma once

// Auto-reconnecting wrapper over gRPC server-side streams:
// PositionsStream, PortfolioStream, TradesStream, OrderStateStream and
// MarketDataServerSideStream. On disconnect the stream is re-established
// with exponential backoff and the original request is re-sent.

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_callback.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "tinvest/client.hpp"
#include "tinvest/detail/unary.hpp"
#include "tinvest/pb_grpc.hpp"

namespace tinvest {

enum class StreamState {
  connecting,    // (re)establishing the stream
  connected,     // first message received
  disconnected,  // stream broke; reconnect scheduled (error attached)
  stopped,       // stop() called or stream destroyed
};

// error is non-null only for `disconnected`. Invoked on SDK/gRPC threads.
using StreamStateCallback =
    std::function<void(StreamState, const Error* error)>;

struct StreamOptions {
  std::chrono::milliseconds initial_backoff{100};
  std::chrono::milliseconds max_backoff{30'000};
  bool reconnect = true;  // false: stop permanently after the first break
};

template <class Req, class Resp>
class ServerStream {
 public:
  using DataCallback = std::function<void(const Resp&)>;
  // Binds the concrete stub method, e.g. stub->async()->PositionsStream(...).
  using StartFn = std::function<void(grpc::ClientContext*, const Req*,
                                     grpc::ClientReadReactor<Resp>*)>;
  using PingFilter = std::function<bool(const Resp&)>;

  ServerStream(Client& client, Req request, StartFn start, DataCallback on_data,
               StreamStateCallback on_state = {}, StreamOptions opts = {})
      : client_(&client),
        request_(std::move(request)),
        start_fn_(std::move(start)),
        on_data_(std::move(on_data)),
        on_state_(std::move(on_state)),
        opts_(opts) {}

  ~ServerStream() { stop(); }
  ServerStream(const ServerStream&) = delete;
  ServerStream& operator=(const ServerStream&) = delete;

  // Messages matching the filter (server pings) are consumed silently.
  void set_ping_filter(PingFilter f) { is_ping_ = std::move(f); }

  void start() {
    std::lock_guard lk(mu_);
    if (worker_.joinable()) return;
    stopping_ = false;
    worker_ = std::thread([this] { run(); });
  }

  void stop() {
    std::thread t;
    {
      std::lock_guard lk(mu_);
      stopping_ = true;
      if (ctx_) ctx_->TryCancel();
      cv_.notify_all();
      t = std::move(worker_);
    }
    if (t.joinable()) t.join();
  }

 private:
  class Reactor : public grpc::ClientReadReactor<Resp> {
   public:
    explicit Reactor(ServerStream* owner) : owner_(owner) {}
    void Begin() {
      this->StartRead(&resp_);
      this->StartCall();
    }
    void OnReadDone(bool ok) override {
      if (!ok) return;
      owner_->handle_data(resp_);
      this->StartRead(&resp_);
    }
    void OnDone(const grpc::Status& status) override {
      owner_->handle_done(status);
    }

   private:
    ServerStream* owner_;
    Resp resp_;
  };

  void run() {
    int attempt = 0;
    while (true) {
      {
        std::lock_guard lk(mu_);
        if (stopping_) break;
      }
      notify(StreamState::connecting, nullptr);
      got_data_ = false;
      const grpc::Status status = run_once();
      {
        std::lock_guard lk(mu_);
        if (stopping_) break;
      }
      if (got_data_) attempt = 0;  // the connection was healthy; reset backoff

      Error err;
      err.code = status.error_code();
      err.message = status.error_message();
      notify(StreamState::disconnected, &err);
      if (!opts_.reconnect) break;

      auto delay = opts_.initial_backoff * (1 << std::min(attempt, 8));
      if (delay > opts_.max_backoff) delay = opts_.max_backoff;
      ++attempt;
      std::unique_lock lk(mu_);
      cv_.wait_for(lk, delay, [this] { return stopping_; });
      if (stopping_) break;
    }
    notify(StreamState::stopped, nullptr);
  }

  grpc::Status run_once() {
    auto ctx = std::make_unique<grpc::ClientContext>();
    client_->setup_context(*ctx, {});
    Reactor reactor(this);
    {
      std::lock_guard lk(mu_);
      if (stopping_) return grpc::Status::CANCELLED;
      ctx_ = ctx.get();
      done_ = false;
    }
    start_fn_(ctx.get(), &request_, &reactor);
    reactor.Begin();
    std::unique_lock lk(mu_);
    cv_.wait(lk, [this] { return done_; });
    ctx_ = nullptr;
    return status_;
  }

  void handle_data(const Resp& resp) {
    if (!got_data_) {
      got_data_ = true;
      notify(StreamState::connected, nullptr);
    }
    if (is_ping_ && is_ping_(resp)) return;
    if (on_data_) on_data_(resp);
  }

  void handle_done(const grpc::Status& status) {
    std::lock_guard lk(mu_);
    status_ = status;
    done_ = true;
    cv_.notify_all();
  }

  void notify(StreamState s, const Error* e) {
    if (on_state_) on_state_(s, e);
  }

  Client* client_;
  Req request_;
  StartFn start_fn_;
  DataCallback on_data_;
  StreamStateCallback on_state_;
  StreamOptions opts_;
  PingFilter is_ping_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::thread worker_;
  bool stopping_ = false;
  bool done_ = false;
  std::atomic<bool> got_data_{false};
  grpc::Status status_;
  grpc::ClientContext* ctx_ = nullptr;
};

using PositionsStream =
    ServerStream<pb::PositionsStreamRequest, pb::PositionsStreamResponse>;
using PortfolioStream =
    ServerStream<pb::PortfolioStreamRequest, pb::PortfolioStreamResponse>;
using TradesStream =
    ServerStream<pb::TradesStreamRequest, pb::TradesStreamResponse>;
using OrderStateStream =
    ServerStream<pb::OrderStateStreamRequest, pb::OrderStateStreamResponse>;
using MarketDataServerStream =
    ServerStream<pb::MarketDataServerSideStreamRequest, pb::MarketDataResponse>;

namespace detail {

// Keeps the per-stream stub alive inside the StartFn closure.
template <class PbService, class Req, class Resp, class Method>
typename ServerStream<Req, Resp>::StartFn bind_stream(
    const std::shared_ptr<grpc::Channel>& channel, Method method) {
  std::shared_ptr<typename PbService::Stub> stub = PbService::NewStub(channel);
  return [stub, method](grpc::ClientContext* ctx, const Req* req,
                        grpc::ClientReadReactor<Resp>* reactor) {
    (stub->async()->*method)(ctx, req, reactor);
  };
}

}  // namespace detail

// Factory helpers: create, configure ping filtering and start the stream.

inline std::unique_ptr<PositionsStream> make_positions_stream(
    Client& client, pb::PositionsStreamRequest req,
    PositionsStream::DataCallback on_data, StreamStateCallback on_state = {},
    StreamOptions opts = {}) {
  auto s = std::make_unique<PositionsStream>(
      client, std::move(req),
      detail::bind_stream<pb::OperationsStreamService,
                          pb::PositionsStreamRequest,
                          pb::PositionsStreamResponse>(
          client.channel(),
          &pb::OperationsStreamService::Stub::async_interface::PositionsStream),
      std::move(on_data), std::move(on_state), opts);
  s->set_ping_filter([](const auto& r) { return r.has_ping(); });
  s->start();
  return s;
}

inline std::unique_ptr<PortfolioStream> make_portfolio_stream(
    Client& client, pb::PortfolioStreamRequest req,
    PortfolioStream::DataCallback on_data, StreamStateCallback on_state = {},
    StreamOptions opts = {}) {
  auto s = std::make_unique<PortfolioStream>(
      client, std::move(req),
      detail::bind_stream<pb::OperationsStreamService,
                          pb::PortfolioStreamRequest,
                          pb::PortfolioStreamResponse>(
          client.channel(),
          &pb::OperationsStreamService::Stub::async_interface::PortfolioStream),
      std::move(on_data), std::move(on_state), opts);
  s->set_ping_filter([](const auto& r) { return r.has_ping(); });
  s->start();
  return s;
}

inline std::unique_ptr<TradesStream> make_trades_stream(
    Client& client, pb::TradesStreamRequest req,
    TradesStream::DataCallback on_data, StreamStateCallback on_state = {},
    StreamOptions opts = {}) {
  auto s = std::make_unique<TradesStream>(
      client, std::move(req),
      detail::bind_stream<pb::OrdersStreamService, pb::TradesStreamRequest,
                          pb::TradesStreamResponse>(
          client.channel(),
          &pb::OrdersStreamService::Stub::async_interface::TradesStream),
      std::move(on_data), std::move(on_state), opts);
  s->set_ping_filter([](const auto& r) { return r.has_ping(); });
  s->start();
  return s;
}

inline std::unique_ptr<OrderStateStream> make_order_state_stream(
    Client& client, pb::OrderStateStreamRequest req,
    OrderStateStream::DataCallback on_data, StreamStateCallback on_state = {},
    StreamOptions opts = {}) {
  auto s = std::make_unique<OrderStateStream>(
      client, std::move(req),
      detail::bind_stream<pb::OrdersStreamService, pb::OrderStateStreamRequest,
                          pb::OrderStateStreamResponse>(
          client.channel(),
          &pb::OrdersStreamService::Stub::async_interface::OrderStateStream),
      std::move(on_data), std::move(on_state), opts);
  s->set_ping_filter([](const auto& r) { return r.has_ping(); });
  s->start();
  return s;
}

inline std::unique_ptr<MarketDataServerStream> make_market_data_server_stream(
    Client& client, pb::MarketDataServerSideStreamRequest req,
    MarketDataServerStream::DataCallback on_data,
    StreamStateCallback on_state = {}, StreamOptions opts = {}) {
  auto s = std::make_unique<MarketDataServerStream>(
      client, std::move(req),
      detail::bind_stream<pb::MarketDataStreamService,
                          pb::MarketDataServerSideStreamRequest,
                          pb::MarketDataResponse>(
          client.channel(), &pb::MarketDataStreamService::Stub::
                                async_interface::MarketDataServerSideStream),
      std::move(on_data), std::move(on_state), opts);
  s->set_ping_filter([](const auto& r) { return r.has_ping(); });
  s->start();
  return s;
}

}  // namespace tinvest
