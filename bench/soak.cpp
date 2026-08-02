// Stability soak test against an in-process mock server:
//   * 4 threads of blocking calls (success + error paths)
//   * an async pump keeping 128 requests in flight
//   * a persistent market-data stream whose server drops the connection
//     every 500 messages (exercises reconnect + resubscribe continuously)
//   * a churn thread creating/subscribing/destroying a stream and a whole
//     Client every cycle (lifecycle leaks)
// Prints counters and VmRSS every 5 s; run under ASan/valgrind for leaks.
// Usage: soak [seconds]  (default 60)

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;
using namespace std::chrono_literals;

namespace {

long rss_kb() {
  std::ifstream f("/proc/self/status");
  std::string line;
  while (std::getline(f, line))
    if (line.rfind("VmRSS:", 0) == 0)
      return std::atol(line.c_str() + 6);
  return -1;
}

class SoakUsers final : public pb::UsersService::Service {
 public:
  grpc::Status GetAccounts(grpc::ServerContext*, const pb::GetAccountsRequest*,
                           pb::GetAccountsResponse* resp) override {
    resp->add_accounts()->set_id("soak");
    return grpc::Status::OK;
  }
  grpc::Status GetMarginAttributes(
      grpc::ServerContext* ctx, const pb::GetMarginAttributesRequest*,
      pb::GetMarginAttributesResponse*) override {
    ctx->AddTrailingMetadata("message", "soak error path");
    ctx->AddTrailingMetadata("x-tracking-id", "soak-trk");
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "30042");
  }
};

// Pushes candles and drops the stream every `drop_every` messages.
class SoakMarketData final : public pb::MarketDataStreamService::Service {
 public:
  grpc::Status MarketDataStream(
      grpc::ServerContext*,
      grpc::ServerReaderWriter<pb::MarketDataResponse, pb::MarketDataRequest>*
          stream) override {
    ++connections;
    pb::MarketDataRequest req;
    while (stream->Read(&req)) {
      if (!req.has_subscribe_candles_request()) continue;
      pb::MarketDataResponse ack;
      ack.mutable_subscribe_candles_response();
      if (!stream->Write(ack)) return grpc::Status::OK;
      pb::MarketDataResponse msg;
      msg.mutable_candle()->set_instrument_uid("uid");
      for (int i = 0; i < kDropEvery; ++i) {
        msg.mutable_candle()->mutable_close()->set_units(i);
        if (!stream->Write(msg)) return grpc::Status::OK;
        ++pushed;
      }
      return grpc::Status::OK;  // forced drop -> client must reconnect
    }
    return grpc::Status::OK;
  }

  static constexpr int kDropEvery = 500;
  std::atomic<long> connections{0};
  std::atomic<long> pushed{0};
};

}  // namespace

int main(int argc, char** argv) {
  const int seconds = argc > 1 ? std::atoi(argv[1]) : 60;

  SoakUsers users;
  SoakMarketData market_data;
  int port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &port);
  builder.RegisterService(&users);
  builder.RegisterService(&market_data);
  auto server = builder.BuildAndStart();
  if (!server || port == 0) {
    std::fprintf(stderr, "failed to start mock server\n");
    return 1;
  }
  const std::string endpoint = "127.0.0.1:" + std::to_string(port);
  auto make_config = [&] {
    tinvest::Config cfg;
    cfg.token = "soak";
    cfg.endpoint = endpoint;
    cfg.use_tls = false;
    return cfg;
  };
  tinvest::Client client(make_config());

  std::atomic<bool> stop{false};
  std::atomic<long> sync_ok{0}, sync_err{0}, async_done{0}, candles{0},
      reconnects{0}, churn_cycles{0};

  // 1) Blocking-call hammer, success and error paths.
  std::vector<std::thread> workers;
  for (int t = 0; t < 4; ++t) {
    workers.emplace_back([&] {
      while (!stop) {
        if (client.users().get_accounts())
          ++sync_ok;
        else
          ++sync_err;
        auto e = client.users().get_margin_attributes();
        if (!e && e.error().tracking_id == "soak-trk") ++sync_ok;
      }
    });
  }

  // 2) Async pump: 128 in flight, forever.
  std::mutex amu;
  std::condition_variable acv;
  int outstanding = 0;
  workers.emplace_back([&] {
    while (!stop) {
      {
        std::unique_lock lk(amu);
        acv.wait_for(lk, 50ms, [&] { return outstanding < 128 || stop; });
        if (stop) break;
        if (outstanding >= 128) continue;
        ++outstanding;
      }
      client.users().get_accounts(pb::GetAccountsRequest{},
                                  [&](auto r) {
                                    (void)r;
                                    std::lock_guard lk(amu);
                                    --outstanding;
                                    ++async_done;
                                    acv.notify_all();
                                  });
    }
    std::unique_lock lk(amu);
    acv.wait_for(lk, 2s, [&] { return outstanding == 0; });
  });

  // 3) Persistent stream living through forced server drops.
  tinvest::MarketDataHandlers handlers;
  handlers.on_candle = [&](const pb::Candle&) { ++candles; };
  auto on_state = [&](tinvest::StreamState s, const tinvest::Error*) {
    if (s == tinvest::StreamState::disconnected) ++reconnects;
  };
  tinvest::StreamOptions fast_retry;
  fast_retry.initial_backoff = 20ms;
  tinvest::MarketDataStream persistent(client, handlers, on_state, fast_retry);
  persistent.start();
  persistent.subscribe_candles({"uid"});

  // 4) Lifecycle churn: fresh Client + stream, subscribe, tear down.
  workers.emplace_back([&] {
    while (!stop) {
      tinvest::Client tmp(make_config());
      {
        tinvest::MarketDataStream s(tmp, {}, {}, fast_retry);
        s.start();
        s.subscribe_last_prices({"uid"});
        std::this_thread::sleep_for(100ms);
        s.stop();
      }
      (void)tmp.users().get_accounts();
      ++churn_cycles;
      std::this_thread::sleep_for(50ms);
    }
  });

  const long rss_start = rss_kb();
  std::printf("soak: %d s, endpoint %s, start RSS %ld KB\n", seconds,
              endpoint.c_str(), rss_start);
  const auto t0 = std::chrono::steady_clock::now();
  long rss_mid_max = 0;
  while (std::chrono::steady_clock::now() - t0 <
         std::chrono::seconds(seconds)) {
    std::this_thread::sleep_for(5s);
    const long rss = rss_kb();
    rss_mid_max = std::max(rss_mid_max, rss);
    std::printf(
        "  t=%3.0fs rss=%6ld KB sync_ok=%ld sync_err=%ld async=%ld "
        "candles=%ld reconnects=%ld churn=%ld\n",
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count(),
        rss, sync_ok.load(), sync_err.load(), async_done.load(),
        candles.load(), reconnects.load(), churn_cycles.load());
    std::fflush(stdout);
  }

  stop = true;
  persistent.stop();
  for (auto& w : workers) w.join();
  server->Shutdown();

  const long rss_end = rss_kb();
  std::printf(
      "soak done: sync_ok=%ld sync_err=%ld async=%ld candles=%ld "
      "reconnects=%ld churn=%ld server_conns=%ld\n",
      sync_ok.load(), sync_err.load(), async_done.load(), candles.load(),
      reconnects.load(), churn_cycles.load(), market_data.connections.load());
  std::printf("RSS: start %ld KB, peak %ld KB, end %ld KB (delta %+ld KB)\n",
              rss_start, rss_mid_max, rss_end, rss_end - rss_start);
  const bool sane = sync_err.load() == 0 && candles.load() > 0 &&
                    reconnects.load() > 0 && churn_cycles.load() > 0;
  return sane ? 0 : 2;
}
