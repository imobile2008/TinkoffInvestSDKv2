// Streams last prices and 1-minute candles for the given instruments.
// Usage: TINVEST_TOKEN=t.xxx ./quotes_stream [instrument_id ...]
// Default instrument: BBG004730N88 (SBER). Runs for 60 seconds.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "tinvest/tinvest.hpp"

int main(int argc, char** argv) {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN environment variable\n");
    return 1;
  }

  std::vector<std::string> ids;
  for (int i = 1; i < argc; ++i) ids.emplace_back(argv[i]);
  if (ids.empty()) ids = {"BBG004730N88"};

  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});

  tinvest::MarketDataHandlers handlers;
  handlers.on_last_price = [](const tinvest::pb::LastPrice& p) {
    std::printf("last_price %s = %s\n", p.instrument_uid().c_str(),
                tinvest::Decimal(p.price()).to_string().c_str());
  };
  handlers.on_candle = [](const tinvest::pb::Candle& c) {
    std::printf("candle %s o=%s h=%s l=%s c=%s v=%lld\n",
                c.instrument_uid().c_str(),
                tinvest::Decimal(c.open()).to_string().c_str(),
                tinvest::Decimal(c.high()).to_string().c_str(),
                tinvest::Decimal(c.low()).to_string().c_str(),
                tinvest::Decimal(c.close()).to_string().c_str(),
                static_cast<long long>(c.volume()));
  };

  auto on_state = [](tinvest::StreamState s, const tinvest::Error* e) {
    const char* names[] = {"connecting", "connected", "disconnected",
                           "stopped"};
    std::printf("[stream] %s%s%s\n", names[static_cast<int>(s)],
                e ? ": " : "", e ? e->to_string().c_str() : "");
  };

  tinvest::MarketDataStream stream(client, handlers, on_state);
  stream.start();
  stream.subscribe_last_prices(ids);
  stream.subscribe_candles(ids, tinvest::pb::SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                           /*waiting_close=*/false);

  std::this_thread::sleep_for(std::chrono::seconds(60));
  stream.stop();
  return 0;
}
