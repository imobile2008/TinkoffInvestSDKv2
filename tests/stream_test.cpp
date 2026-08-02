// Market-data stream tests: subscription delivery, forced disconnect,
// automatic reconnect with re-subscription.

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

namespace {

// First connection: replies to a candles subscription with a confirmation and
// one candle, then closes the stream (simulated drop). Later connections stay
// open until the client cancels.
class MockMarketData final : public pb::MarketDataStreamService::Service {
 public:
  grpc::Status MarketDataStream(
      grpc::ServerContext* ctx,
      grpc::ServerReaderWriter<pb::MarketDataResponse, pb::MarketDataRequest>*
          stream) override {
    const int conn = ++connections_;
    pb::MarketDataRequest req;
    while (stream->Read(&req)) {
      if (!req.has_subscribe_candles_request()) continue;
      const auto& sub = req.subscribe_candles_request();
      if (sub.subscription_action() != pb::SUBSCRIPTION_ACTION_SUBSCRIBE)
        continue;

      pb::MarketDataResponse confirm;
      confirm.mutable_subscribe_candles_response()->set_tracking_id("trk");
      stream->Write(confirm);

      pb::MarketDataResponse candle;
      auto* c = candle.mutable_candle();
      c->set_instrument_uid(sub.instruments(0).instrument_id());
      c->mutable_close()->set_units(100 + conn);
      stream->Write(candle);

      if (conn == 1) return grpc::Status::OK;  // drop the first connection
    }
    return grpc::Status::OK;
  }

  std::atomic<int> connections_{0};
};

class StreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&market_data_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    tinvest::Config cfg;
    cfg.token = "test-token";
    cfg.endpoint = "127.0.0.1:" + std::to_string(port);
    cfg.use_tls = false;
    client_ = std::make_unique<tinvest::Client>(std::move(cfg));
  }

  void TearDown() override {
    client_.reset();
    server_->Shutdown();
  }

  MockMarketData market_data_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<tinvest::Client> client_;
};

TEST_F(StreamTest, ResubscribesAfterDisconnect) {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<long long> closes;
  int subscription_results = 0;

  tinvest::MarketDataHandlers handlers;
  handlers.on_candle = [&](const pb::Candle& c) {
    std::lock_guard lk(mu);
    closes.push_back(c.close().units());
    cv.notify_all();
  };
  handlers.on_subscription_result = [&](const pb::MarketDataResponse&) {
    std::lock_guard lk(mu);
    ++subscription_results;
  };

  tinvest::StreamOptions opts;
  opts.initial_backoff = std::chrono::milliseconds(50);

  tinvest::MarketDataStream stream(*client_, handlers, {}, opts);
  stream.start();
  stream.subscribe_candles({"uid-1"});

  {
    std::unique_lock lk(mu);
    // Candle from connection 1 (close=101), then after the forced drop the
    // stream must reconnect, replay the subscription and deliver close=102.
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(20),
                            [&] { return closes.size() >= 2; }))
        << "got " << closes.size() << " candles";
    EXPECT_EQ(closes[0], 101);
    EXPECT_EQ(closes[1], 102);
    EXPECT_GE(subscription_results, 2);  // confirmation on each connection
  }
  EXPECT_GE(market_data_.connections_.load(), 2);
  stream.stop();
}

TEST_F(StreamTest, StopIsIdempotentAndFast) {
  tinvest::MarketDataStream stream(*client_, {}, {});
  stream.start();
  stream.subscribe_last_prices({"uid-1"});
  stream.stop();
  stream.stop();  // second stop must be a no-op
}

}  // namespace
