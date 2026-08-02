// Microbenchmarks: Decimal math, strategy/backtest throughput, and SDK+gRPC
// overhead over an in-process loopback server (no real network involved).

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <numeric>
#include <vector>

#include "tinvest/algo/backtest.hpp"
#include "tinvest/algo/strategies.hpp"
#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;
using Clock = std::chrono::steady_clock;

namespace {

double seconds_since(Clock::time_point start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}

double percentile(std::vector<double> v, double p) {
  std::sort(v.begin(), v.end());
  const auto idx = static_cast<std::size_t>(p / 100.0 * (v.size() - 1));
  return v[idx];
}

void bench_decimal() {
  constexpr int kN = 10'000'000;
  tinvest::Decimal acc(0);
  const tinvest::Decimal step(1, 234'567'891);
  auto start = Clock::now();
  for (int i = 0; i < kN; ++i) acc += step;
  const double add_s = seconds_since(start);

  tinvest::Decimal prod(1, 1);
  const tinvest::Decimal k(1, 1);  // 1.000000001
  start = Clock::now();
  for (int i = 0; i < kN; ++i) prod = prod * k;
  const double mul_s = seconds_since(start);

  std::printf("Decimal        add %.1f ns/op, mul %.1f ns/op  (checksum %s)\n",
              add_s / kN * 1e9, mul_s / kN * 1e9,
              (acc + prod).to_string().substr(0, 6).c_str());
}

void bench_backtest() {
  constexpr int kBars = 1'000'000;
  std::vector<tinvest::algo::Bar> bars;
  bars.reserve(kBars);
  for (int i = 0; i < kBars; ++i) {
    const double base = 100.0 + 10.0 * std::sin(i / 50.0);
    tinvest::algo::Bar b{};
    b.time = i;
    b.open = base;
    b.high = base + 1;
    b.low = base - 1;
    b.close = base + 0.2;
    bars.push_back(b);
  }
  tinvest::algo::SmaCross strat(20, 50);
  auto start = Clock::now();
  const auto report = tinvest::algo::run_backtest(strat, bars);
  const double s = seconds_since(start);
  std::printf("Backtest       %.1f M bars/s (SmaCross, %d trades)\n",
              kBars / s / 1e6, report.trades);
}

class BenchUsers final : public pb::UsersService::Service {
 public:
  grpc::Status GetAccounts(grpc::ServerContext*, const pb::GetAccountsRequest*,
                           pb::GetAccountsResponse* resp) override {
    resp->add_accounts()->set_id("bench");
    return grpc::Status::OK;
  }
};

class BenchMarketData final : public pb::MarketDataStreamService::Service {
 public:
  explicit BenchMarketData(int messages) : messages_(messages) {}
  grpc::Status MarketDataStream(
      grpc::ServerContext*,
      grpc::ServerReaderWriter<pb::MarketDataResponse, pb::MarketDataRequest>*
          stream) override {
    pb::MarketDataRequest req;
    while (stream->Read(&req)) {
      if (!req.has_subscribe_candles_request()) continue;
      pb::MarketDataResponse ack;
      ack.mutable_subscribe_candles_response();
      stream->Write(ack);
      pb::MarketDataResponse msg;
      auto* candle = msg.mutable_candle();
      candle->set_instrument_uid("uid");
      candle->mutable_close()->set_nano(123'000'000);
      for (int i = 0; i < messages_; ++i) {
        candle->mutable_close()->set_units(i);
        if (!stream->Write(msg)) return grpc::Status::OK;
      }
      return grpc::Status::OK;
    }
    return grpc::Status::OK;
  }

 private:
  int messages_;
};

struct LocalServer {
  BenchUsers users;
  BenchMarketData market_data;
  std::unique_ptr<grpc::Server> server;
  int port = 0;

  explicit LocalServer(int stream_messages) : market_data(stream_messages) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&users);
    builder.RegisterService(&market_data);
    server = builder.BuildAndStart();
  }
  ~LocalServer() { server->Shutdown(); }
};

void bench_rpc(tinvest::Client& client) {
  // Sync latency.
  constexpr int kWarmup = 200, kIters = 3000;
  for (int i = 0; i < kWarmup; ++i) (void)client.users().get_accounts();
  std::vector<double> us;
  us.reserve(kIters);
  for (int i = 0; i < kIters; ++i) {
    const auto start = Clock::now();
    auto r = client.users().get_accounts();
    if (!r) std::abort();
    us.push_back(seconds_since(start) * 1e6);
  }
  std::printf(
      "Unary sync     p50 %.0f us, p90 %.0f us, p99 %.0f us (loopback)\n",
      percentile(us, 50), percentile(us, 90), percentile(us, 99));

  // Async pipelined throughput.
  constexpr int kTotal = 50'000, kWindow = 256;
  std::mutex mu;
  std::condition_variable cv;
  int outstanding = 0, completed = 0, failed = 0;
  const auto start = Clock::now();
  for (int i = 0; i < kTotal; ++i) {
    {
      std::unique_lock lk(mu);
      cv.wait(lk, [&] { return outstanding < kWindow; });
      ++outstanding;
    }
    client.users().get_accounts(
        pb::GetAccountsRequest{},
        [&](tinvest::Result<pb::GetAccountsResponse> r) {
          std::lock_guard lk(mu);
          --outstanding;
          ++completed;
          if (!r) ++failed;
          cv.notify_all();
        });
  }
  {
    std::unique_lock lk(mu);
    cv.wait(lk, [&] { return completed == kTotal; });
  }
  const double s = seconds_since(start);
  std::printf("Unary async    %.0f req/s (window %d, failures %d)\n",
              kTotal / s, kWindow, failed);
}

void bench_stream(tinvest::Client& client, int messages) {
  std::mutex mu;
  std::condition_variable cv;
  int received = 0;
  tinvest::MarketDataHandlers handlers;
  handlers.on_candle = [&](const pb::Candle&) {
    std::lock_guard lk(mu);
    if (++received == messages) cv.notify_all();
  };
  tinvest::StreamOptions opts;
  opts.reconnect = false;
  tinvest::MarketDataStream stream(client, handlers, {}, opts);
  stream.start();
  const auto start = Clock::now();
  stream.subscribe_candles({"uid"});
  {
    std::unique_lock lk(mu);
    cv.wait(lk, [&] { return received >= messages; });
  }
  const double s = seconds_since(start);
  stream.stop();
  std::printf("Stream         %.0f k msg/s (%d candles, loopback)\n",
              messages / s / 1e3, messages);
}

}  // namespace

int main() {
  bench_decimal();
  bench_backtest();

  constexpr int kStreamMessages = 200'000;
  LocalServer server(kStreamMessages);
  tinvest::Config cfg;
  cfg.token = "bench";
  cfg.endpoint = "127.0.0.1:" + std::to_string(server.port);
  cfg.use_tls = false;
  tinvest::Client client(std::move(cfg));

  bench_rpc(client);
  bench_stream(client, kStreamMessages);
  return 0;
}
