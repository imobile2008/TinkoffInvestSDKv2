#include "tinvest/streams/market_data_stream.hpp"

#include <algorithm>
#include <utility>

namespace tinvest {

void dispatch(const MarketDataHandlers& h, const pb::MarketDataResponse& r) {
  if (h.on_raw) h.on_raw(r);
  switch (r.payload_case()) {
    case pb::MarketDataResponse::kCandle:
      if (h.on_candle) h.on_candle(r.candle());
      break;
    case pb::MarketDataResponse::kTrade:
      if (h.on_trade) h.on_trade(r.trade());
      break;
    case pb::MarketDataResponse::kOrderbook:
      if (h.on_order_book) h.on_order_book(r.orderbook());
      break;
    case pb::MarketDataResponse::kLastPrice:
      if (h.on_last_price) h.on_last_price(r.last_price());
      break;
    case pb::MarketDataResponse::kTradingStatus:
      if (h.on_trading_status) h.on_trading_status(r.trading_status());
      break;
    case pb::MarketDataResponse::kOpenInterest:
      if (h.on_open_interest) h.on_open_interest(r.open_interest());
      break;
    case pb::MarketDataResponse::kSubscribeCandlesResponse:
    case pb::MarketDataResponse::kSubscribeOrderBookResponse:
    case pb::MarketDataResponse::kSubscribeTradesResponse:
    case pb::MarketDataResponse::kSubscribeInfoResponse:
    case pb::MarketDataResponse::kSubscribeLastPriceResponse:
      if (h.on_subscription_result) h.on_subscription_result(r);
      break;
    case pb::MarketDataResponse::kPing:
    default:
      break;
  }
}

class MarketDataStream::Reactor
    : public grpc::ClientBidiReactor<pb::MarketDataRequest,
                                     pb::MarketDataResponse> {
 public:
  explicit Reactor(MarketDataStream* owner) : owner_(owner) {}

  void Begin() {
    // Writes are initiated from user threads, so the call must be held open
    // between them; the hold is released on stop or stream breakage.
    AddHold();
    StartRead(&response_);
    StartCall();
  }

  // Called with the owner mutex held.
  void WriteNext(pb::MarketDataRequest request) {
    request_ = std::move(request);
    StartWrite(&request_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      owner_->handle_read_closed();
      return;
    }
    owner_->handle_response(response_);
    StartRead(&response_);
  }

  void OnWriteDone(bool ok) override { owner_->handle_write_done(ok); }

  void OnDone(const grpc::Status& status) override {
    owner_->handle_done(status);
  }

 private:
  MarketDataStream* owner_;
  pb::MarketDataRequest request_;
  pb::MarketDataResponse response_;
};

MarketDataStream::MarketDataStream(Client& client, MarketDataHandlers handlers,
                                   StreamStateCallback on_state,
                                   StreamOptions opts)
    : client_(&client),
      handlers_(std::move(handlers)),
      on_state_(std::move(on_state)),
      opts_(opts),
      stub_(pb::MarketDataStreamService::NewStub(client.channel())) {}

MarketDataStream::~MarketDataStream() { stop(); }

void MarketDataStream::start() {
  std::lock_guard lk(mu_);
  if (worker_.joinable()) return;
  stopping_ = false;
  worker_ = std::thread([this] { run(); });
}

void MarketDataStream::stop() {
  std::thread t;
  Reactor* release = nullptr;
  {
    std::lock_guard lk(mu_);
    stopping_ = true;
    broken_ = true;
    if (reactor_ && !hold_released_) {
      hold_released_ = true;
      release = reactor_;
    }
    if (ctx_) ctx_->TryCancel();
    cv_.notify_all();
    t = std::move(worker_);
  }
  if (release) release->RemoveHold();
  if (t.joinable()) t.join();
}

void MarketDataStream::run() {
  int attempt = 0;
  while (true) {
    {
      std::lock_guard lk(mu_);
      if (stopping_) break;
    }
    notify(StreamState::connecting, nullptr);
    const grpc::Status status = run_once();
    {
      std::lock_guard lk(mu_);
      if (stopping_) break;
    }
    if (got_data_) attempt = 0;

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

grpc::Status MarketDataStream::run_once() {
  auto ctx = std::make_unique<grpc::ClientContext>();
  client_->setup_context(*ctx, {});
  Reactor reactor(this);
  {
    std::lock_guard lk(mu_);
    if (stopping_) return grpc::Status::CANCELLED;
    ctx_ = ctx.get();
    done_ = false;
    broken_ = false;
    hold_released_ = false;
    got_data_ = false;
    write_in_flight_ = false;
    // Replay the whole subscription registry on every (re)connect.
    write_queue_.clear();
    for (const auto& [key, request] : subscriptions_)
      write_queue_.push_back(request);
    // reactor_ is NOT published yet: a concurrent subscribe_*() must not be
    // able to StartWrite before the reactor is bound to the call below.
  }
  stub_->async()->MarketDataStream(ctx.get(), &reactor);
  reactor.Begin();
  // Publish the reactor only now that the call is bound and started. If the
  // stream broke or stop() arrived meanwhile, we own the hold release.
  Reactor* release = nullptr;
  {
    std::lock_guard lk(mu_);
    if (!done_) {
      reactor_ = &reactor;
      if ((stopping_ || broken_) && !hold_released_) {
        hold_released_ = true;
        release = &reactor;
      } else {
        maybe_write_locked();
      }
    }
  }
  if (release) release->RemoveHold();
  std::unique_lock lk(mu_);
  cv_.wait(lk, [this] { return done_; });
  ctx_ = nullptr;
  return status_;
}

void MarketDataStream::maybe_write_locked() {
  if (!reactor_ || broken_ || hold_released_ || write_in_flight_ ||
      write_queue_.empty())
    return;
  write_in_flight_ = true;
  pb::MarketDataRequest next = std::move(write_queue_.front());
  write_queue_.pop_front();
  reactor_->WriteNext(std::move(next));
}

void MarketDataStream::handle_write_done(bool ok) {
  if (!ok) {
    handle_read_closed();  // same treatment: stream is broken
    return;
  }
  std::lock_guard lk(mu_);
  write_in_flight_ = false;
  maybe_write_locked();
}

void MarketDataStream::handle_read_closed() {
  Reactor* release = nullptr;
  {
    std::lock_guard lk(mu_);
    broken_ = true;
    if (reactor_ && !hold_released_) {
      hold_released_ = true;
      release = reactor_;
    }
  }
  if (release) release->RemoveHold();
}

void MarketDataStream::handle_response(const pb::MarketDataResponse& response) {
  bool first = false;
  {
    std::lock_guard lk(mu_);
    if (!got_data_) {
      got_data_ = true;
      first = true;
    }
  }
  if (first) notify(StreamState::connected, nullptr);
  dispatch(handlers_, response);
}

void MarketDataStream::handle_done(const grpc::Status& status) {
  std::lock_guard lk(mu_);
  reactor_ = nullptr;
  status_ = status;
  done_ = true;
  cv_.notify_all();
}

void MarketDataStream::notify(StreamState s, const Error* e) {
  if (on_state_) on_state_(s, e);
}

// --- subscription registry ---------------------------------------------------

namespace {

pb::MarketDataRequest single(const pb::MarketDataRequest& src,
                             int instrument_index) {
  pb::MarketDataRequest one;
  switch (src.payload_case()) {
    case pb::MarketDataRequest::kSubscribeCandlesRequest: {
      auto* p = one.mutable_subscribe_candles_request();
      *p = src.subscribe_candles_request();
      auto inst = p->instruments(instrument_index);
      p->clear_instruments();
      *p->add_instruments() = inst;
      break;
    }
    case pb::MarketDataRequest::kSubscribeOrderBookRequest: {
      auto* p = one.mutable_subscribe_order_book_request();
      *p = src.subscribe_order_book_request();
      auto inst = p->instruments(instrument_index);
      p->clear_instruments();
      *p->add_instruments() = inst;
      break;
    }
    case pb::MarketDataRequest::kSubscribeTradesRequest: {
      auto* p = one.mutable_subscribe_trades_request();
      *p = src.subscribe_trades_request();
      auto inst = p->instruments(instrument_index);
      p->clear_instruments();
      *p->add_instruments() = inst;
      break;
    }
    case pb::MarketDataRequest::kSubscribeInfoRequest: {
      auto* p = one.mutable_subscribe_info_request();
      *p = src.subscribe_info_request();
      auto inst = p->instruments(instrument_index);
      p->clear_instruments();
      *p->add_instruments() = inst;
      break;
    }
    case pb::MarketDataRequest::kSubscribeLastPriceRequest: {
      auto* p = one.mutable_subscribe_last_price_request();
      *p = src.subscribe_last_price_request();
      auto inst = p->instruments(instrument_index);
      p->clear_instruments();
      *p->add_instruments() = inst;
      break;
    }
    default:
      break;
  }
  return one;
}

// Registry keys identify a subscription target, not its options: repeated
// subscribe calls for the same target overwrite the stored request.
std::vector<std::string> keys_of(const pb::MarketDataRequest& r) {
  std::vector<std::string> keys;
  switch (r.payload_case()) {
    case pb::MarketDataRequest::kSubscribeCandlesRequest:
      for (const auto& i : r.subscribe_candles_request().instruments()) {
        const std::string& id = i.instrument_id().empty() ? i.figi()
                                                          : i.instrument_id();
        keys.push_back("candles|" + id + "|" +
                       std::to_string(static_cast<int>(i.interval())));
      }
      break;
    case pb::MarketDataRequest::kSubscribeOrderBookRequest:
      for (const auto& i : r.subscribe_order_book_request().instruments()) {
        const std::string& id = i.instrument_id().empty() ? i.figi()
                                                          : i.instrument_id();
        keys.push_back(
            "orderbook|" + id + "|" + std::to_string(i.depth()) + "|" +
            std::to_string(static_cast<int>(i.order_book_type())));
      }
      break;
    case pb::MarketDataRequest::kSubscribeTradesRequest:
      for (const auto& i : r.subscribe_trades_request().instruments()) {
        const std::string& id = i.instrument_id().empty() ? i.figi()
                                                          : i.instrument_id();
        keys.push_back("trades|" + id);
      }
      break;
    case pb::MarketDataRequest::kSubscribeInfoRequest:
      for (const auto& i : r.subscribe_info_request().instruments()) {
        const std::string& id = i.instrument_id().empty() ? i.figi()
                                                          : i.instrument_id();
        keys.push_back("info|" + id);
      }
      break;
    case pb::MarketDataRequest::kSubscribeLastPriceRequest:
      for (const auto& i : r.subscribe_last_price_request().instruments()) {
        const std::string& id = i.instrument_id().empty() ? i.figi()
                                                          : i.instrument_id();
        keys.push_back("last_price|" + id);
      }
      break;
    default:
      break;
  }
  return keys;
}

pb::SubscriptionAction action_of(const pb::MarketDataRequest& r) {
  switch (r.payload_case()) {
    case pb::MarketDataRequest::kSubscribeCandlesRequest:
      return r.subscribe_candles_request().subscription_action();
    case pb::MarketDataRequest::kSubscribeOrderBookRequest:
      return r.subscribe_order_book_request().subscription_action();
    case pb::MarketDataRequest::kSubscribeTradesRequest:
      return r.subscribe_trades_request().subscription_action();
    case pb::MarketDataRequest::kSubscribeInfoRequest:
      return r.subscribe_info_request().subscription_action();
    case pb::MarketDataRequest::kSubscribeLastPriceRequest:
      return r.subscribe_last_price_request().subscription_action();
    default:
      return pb::SUBSCRIPTION_ACTION_UNSPECIFIED;
  }
}

}  // namespace

void MarketDataStream::remember_locked(const pb::MarketDataRequest& request) {
  const auto keys = keys_of(request);
  for (std::size_t i = 0; i < keys.size(); ++i)
    subscriptions_[keys[i]] = single(request, static_cast<int>(i));
}

void MarketDataStream::forget_locked(const pb::MarketDataRequest& request) {
  for (const auto& key : keys_of(request)) subscriptions_.erase(key);
}

void MarketDataStream::send(pb::MarketDataRequest request, bool remember) {
  std::lock_guard lk(mu_);
  if (stopping_) return;
  if (remember) {
    const auto action = action_of(request);
    if (action == pb::SUBSCRIPTION_ACTION_SUBSCRIBE)
      remember_locked(request);
    else if (action == pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE)
      forget_locked(request);
  }
  // With the stream currently broken the registry update above is enough:
  // the subscription is replayed after reconnect. Queue only onto a live one.
  if (reactor_ && !broken_ && !hold_released_) {
    write_queue_.push_back(std::move(request));
    maybe_write_locked();
  }
}

// --- convenience subscriptions ----------------------------------------------

void MarketDataStream::subscribe_candles(
    const std::vector<std::string>& ids, pb::SubscriptionInterval interval,
    bool waiting_close) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_candles_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_SUBSCRIBE);
  p->set_waiting_close(waiting_close);
  for (const auto& id : ids) {
    auto* inst = p->add_instruments();
    inst->set_instrument_id(id);
    inst->set_interval(interval);
  }
  send(std::move(req));
}

void MarketDataStream::unsubscribe_candles(const std::vector<std::string>& ids,
                                           pb::SubscriptionInterval interval) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_candles_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE);
  for (const auto& id : ids) {
    auto* inst = p->add_instruments();
    inst->set_instrument_id(id);
    inst->set_interval(interval);
  }
  send(std::move(req));
}

void MarketDataStream::subscribe_order_book(const std::vector<std::string>& ids,
                                            int depth,
                                            pb::OrderBookType type) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_order_book_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_SUBSCRIBE);
  for (const auto& id : ids) {
    auto* inst = p->add_instruments();
    inst->set_instrument_id(id);
    inst->set_depth(depth);
    inst->set_order_book_type(type);
  }
  send(std::move(req));
}

void MarketDataStream::unsubscribe_order_book(
    const std::vector<std::string>& ids, int depth, pb::OrderBookType type) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_order_book_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE);
  for (const auto& id : ids) {
    auto* inst = p->add_instruments();
    inst->set_instrument_id(id);
    inst->set_depth(depth);
    inst->set_order_book_type(type);
  }
  send(std::move(req));
}

void MarketDataStream::subscribe_trades(const std::vector<std::string>& ids,
                                        bool with_open_interest) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_trades_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_SUBSCRIBE);
  p->set_with_open_interest(with_open_interest);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::unsubscribe_trades(const std::vector<std::string>& ids) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_trades_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::subscribe_last_prices(
    const std::vector<std::string>& ids) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_last_price_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_SUBSCRIBE);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::unsubscribe_last_prices(
    const std::vector<std::string>& ids) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_last_price_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::subscribe_info(const std::vector<std::string>& ids) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_info_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_SUBSCRIBE);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::unsubscribe_info(const std::vector<std::string>& ids) {
  pb::MarketDataRequest req;
  auto* p = req.mutable_subscribe_info_request();
  p->set_subscription_action(pb::SUBSCRIPTION_ACTION_UNSUBSCRIBE);
  for (const auto& id : ids) p->add_instruments()->set_instrument_id(id);
  send(std::move(req));
}

void MarketDataStream::get_my_subscriptions() {
  pb::MarketDataRequest req;
  req.mutable_get_my_subscriptions();
  send(std::move(req), /*remember=*/false);
}

}  // namespace tinvest
