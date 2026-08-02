// Backtests the classic strategies on real daily history from the API.
// Usage: TINVEST_TOKEN=t.xxx ./backtest [instrument_id] [days]
// Default: BBG004730N88 (SBER), 365 days of daily candles.

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "tinvest/algo/backtest.hpp"
#include "tinvest/algo/strategies.hpp"
#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;
using namespace tinvest::algo;

int main(int argc, char** argv) {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN environment variable\n");
    return 1;
  }
  const std::string instrument = argc > 1 ? argv[1] : "BBG004730N88";
  const int days = argc > 2 ? std::atoi(argv[2]) : 365;

  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});

  pb::GetCandlesRequest req;
  req.set_instrument_id(instrument);
  req.set_interval(pb::CANDLE_INTERVAL_DAY);
  const std::time_t now = std::time(nullptr);
  req.mutable_from()->set_seconds(now - static_cast<std::time_t>(days) * 86400);
  req.mutable_to()->set_seconds(now);

  auto candles = client.market_data().get_candles(req);
  if (!candles) {
    std::fprintf(stderr, "GetCandles failed: %s\n",
                 candles.error().to_string().c_str());
    return 1;
  }

  std::vector<Bar> bars;
  for (const auto& c : candles->candles())
    if (c.is_complete()) bars.push_back(to_bar(c));
  if (bars.size() < 60) {
    std::fprintf(stderr, "Not enough candles: %zu\n", bars.size());
    return 1;
  }
  std::printf("%s: %zu daily bars, %.2f -> %.2f\n\n", instrument.c_str(),
              bars.size(), bars.front().close, bars.back().close);

  std::vector<std::unique_ptr<Strategy>> strategies;
  strategies.push_back(std::make_unique<SmaCross>(20, 50));
  strategies.push_back(std::make_unique<EmaCross>(12, 26));
  strategies.push_back(std::make_unique<RsiReversion>(14, 30, 70));
  strategies.push_back(std::make_unique<BollingerReversion>(20, 2.0));
  strategies.push_back(std::make_unique<MacdCross>(12, 26, 9));
  strategies.push_back(std::make_unique<DonchianBreakout>(20, 10));

  std::printf("%-28s %8s %8s %8s %8s %7s %8s\n", "strategy", "return%",
              "b&h%", "maxDD%", "sharpe", "trades", "win%");
  for (auto& s : strategies) {
    const BacktestReport r = run_backtest(*s, bars, /*commission_pct=*/0.05);
    std::printf("%-28s %8.2f %8.2f %8.2f %8.2f %7d %8.1f\n",
                r.strategy.c_str(), r.total_return_pct, r.buy_hold_return_pct,
                r.max_drawdown_pct, r.sharpe, r.trades, r.win_rate_pct);
  }
  return 0;
}
