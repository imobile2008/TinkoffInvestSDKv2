#pragma once

// Bidirectional market-data stream with automatic reconnection and
// re-subscription. Subscriptions issued through this class are remembered
// and replayed after every reconnect, so the caller subscribes once.

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tinvest/client.hpp"
#include "tinvest/pb_grpc.hpp"
#include "tinvest/streams/server_stream.hpp"  // StreamState, StreamOptions

namespace tinvest {

// Every handler is optional and invoked on a gRPC worker thread: keep them
// short and non-blocking (copy the message and hand it to your own queue if
// processing is heavy).
struct MarketDataHandlers {
  std::function<void(const pb::Candle&)> on_candle;
  std::function<void(const pb::Trade&)> on_trade;
  std::function<void(const pb::OrderBook&)> on_order_book;
  std::function<void(const pb::LastPrice&)> on_last_price;
  std::function<void(const pb::TradingStatus&)> on_trading_status;
  std::function<void(const pb::OpenInterest&)> on_open_interest;
  // subscribe_*_response payloads (results of (un)subscribe requests).
  std::function<void(const pb::MarketDataResponse&)> on_subscription_result;
  // Raw tap: every message including pings, before dispatch.
  std::function<void(const pb::MarketDataResponse&)> on_raw;
};

// Routes a MarketDataResponse to the matching handler. Shared with
// MarketDataServerStream users.
void dispatch(const MarketDataHandlers& handlers,
              const pb::MarketDataResponse& response);

class MarketDataStream {
 public:
  MarketDataStream(Client& client, MarketDataHandlers handlers,
                   StreamStateCallback on_state = {}, StreamOptions opts = {});
  ~MarketDataStream();
  MarketDataStream(const MarketDataStream&) = delete;
  MarketDataStream& operator=(const MarketDataStream&) = delete;

  void start();
  void stop();

  // Convenience subscriptions. instrument_id = figi, instrument_uid or
  // "ticker_classcode".
  void subscribe_candles(
      const std::vector<std::string>& instrument_ids,
      pb::SubscriptionInterval interval =
          pb::SUBSCRIPTION_INTERVAL_ONE_MINUTE,
      bool waiting_close = true);
  void unsubscribe_candles(const std::vector<std::string>& instrument_ids,
                           pb::SubscriptionInterval interval =
                               pb::SUBSCRIPTION_INTERVAL_ONE_MINUTE);
  void subscribe_order_book(
      const std::vector<std::string>& instrument_ids, int depth = 10,
      pb::OrderBookType order_book_type = pb::ORDERBOOK_TYPE_UNSPECIFIED);
  void unsubscribe_order_book(
      const std::vector<std::string>& instrument_ids, int depth = 10,
      pb::OrderBookType order_book_type = pb::ORDERBOOK_TYPE_UNSPECIFIED);
  void subscribe_trades(const std::vector<std::string>& instrument_ids,
                        bool with_open_interest = false);
  void unsubscribe_trades(const std::vector<std::string>& instrument_ids);
  void subscribe_last_prices(const std::vector<std::string>& instrument_ids);
  void unsubscribe_last_prices(const std::vector<std::string>& instrument_ids);
  void subscribe_info(const std::vector<std::string>& instrument_ids);
  void unsubscribe_info(const std::vector<std::string>& instrument_ids);

  // Requests the current subscription list; the reply arrives through
  // on_subscription_result handlers.
  void get_my_subscriptions();

  // Escape hatch: send a raw request. If remember == true, single-instrument
  // subscribe requests derived from it are replayed after reconnects.
  void send(pb::MarketDataRequest request, bool remember = true);

 private:
  class Reactor;
  friend class Reactor;

  void run();
  grpc::Status run_once();
  void maybe_write_locked();
  void remember_locked(const pb::MarketDataRequest& request);
  void forget_locked(const pb::MarketDataRequest& request);
  void handle_response(const pb::MarketDataResponse& response);
  void handle_write_done(bool ok);
  void handle_read_closed();
  void handle_done(const grpc::Status& status);
  void release_hold();
  void notify(StreamState s, const Error* e);

  Client* client_;
  MarketDataHandlers handlers_;
  StreamStateCallback on_state_;
  StreamOptions opts_;
  std::shared_ptr<pb::MarketDataStreamService::Stub> stub_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::thread worker_;
  bool stopping_ = false;
  bool done_ = false;
  bool got_data_ = false;
  bool broken_ = false;         // stream unusable; drop further writes
  bool hold_released_ = false;  // RemoveHold already issued for this reactor
  grpc::Status status_;
  grpc::ClientContext* ctx_ = nullptr;
  Reactor* reactor_ = nullptr;

  std::deque<pb::MarketDataRequest> write_queue_;
  bool write_in_flight_ = false;

  // key -> single-instrument subscribe request, replayed on reconnect.
  std::map<std::string, pb::MarketDataRequest> subscriptions_;
};

}  // namespace tinvest
