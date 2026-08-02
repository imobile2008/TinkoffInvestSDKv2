// Live paper trading: runs an EMA-cross strategy on the real-time last-price
// stream. No orders are ever sent — fills are simulated at the stream price.
// Usage: TINVEST_TOKEN=t.xxx ./paper_trade [instrument_id] [seconds] [fast] [slow]
// Defaults: BBG004730N88 (SBER), 60 s, EMA 20/60 (in ticks).

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>

#include "tinvest/algo/indicators.hpp"
#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

int main(int argc, char** argv) {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN environment variable\n");
    return 1;
  }
  const std::string instrument = argc > 1 ? argv[1] : "BBG004730N88";
  const int seconds = argc > 2 ? std::atoi(argv[2]) : 60;
  const int fast_n = argc > 3 ? std::atoi(argv[3]) : 20;
  const int slow_n = argc > 4 ? std::atoi(argv[4]) : 60;

  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});

  std::mutex mu;
  tinvest::algo::Ema fast(fast_n), slow(slow_n);
  bool prev_above = false, has_prev = false, long_pos = false;
  double entry = 0, realized_pct = 0, last = 0;
  std::atomic<int> ticks{0};

  tinvest::MarketDataHandlers handlers;
  handlers.on_last_price = [&](const pb::LastPrice& p) {
    const double price = tinvest::Decimal(p.price()).to_double();
    std::lock_guard lk(mu);
    last = price;
    ++ticks;
    fast.push(price);
    slow.push(price);
    if (!fast.ready() || !slow.ready()) return;
    const bool above = fast.value() > slow.value();
    if (has_prev) {
      if (above && !prev_above && !long_pos) {
        long_pos = true;
        entry = price;
        std::printf("[paper] BUY  @ %.2f (tick %d)\n", price, ticks.load());
      } else if (!above && prev_above && long_pos) {
        long_pos = false;
        const double pnl = (price / entry - 1.0) * 100.0;
        realized_pct += pnl;
        std::printf("[paper] SELL @ %.2f  pnl %+.3f%%\n", price, pnl);
      }
    }
    prev_above = above;
    has_prev = true;
  };

  auto on_state = [](tinvest::StreamState s, const tinvest::Error* e) {
    const char* names[] = {"connecting", "connected", "disconnected",
                           "stopped"};
    std::printf("[stream] %s%s%s\n", names[static_cast<int>(s)],
                e ? ": " : "", e ? e->to_string().c_str() : "");
  };

  tinvest::MarketDataStream stream(client, handlers, on_state);
  stream.start();
  stream.subscribe_last_prices({instrument});
  stream.subscribe_trades({instrument});  // trades feed ticks too on quiet books

  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  stream.stop();

  std::lock_guard lk(mu);
  double open_pnl = 0;
  if (long_pos && entry > 0 && last > 0) open_pnl = (last / entry - 1.0) * 100.0;
  std::printf(
      "\nticks=%d  realized=%+.3f%%  open=%+.3f%%  total=%+.3f%%  %s\n",
      ticks.load(), realized_pct, open_pnl, realized_pct + open_pnl,
      long_pos ? "(long position open at stop)" : "(flat)");
  return 0;
}
