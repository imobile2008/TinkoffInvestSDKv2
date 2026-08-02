#pragma once

// Minimal long-only backtester. Fills happen at the close of the signal bar,
// a fixed commission is charged per side. Metrics are computed from the
// per-bar equity curve.

#include <cmath>
#include <string>
#include <vector>

#include "tinvest/algo/strategy.hpp"

namespace tinvest::algo {

struct TradeRecord {
  std::int64_t entry_time = 0;
  std::int64_t exit_time = 0;
  double entry_price = 0;
  double exit_price = 0;
  double return_pct = 0;  // net of commissions
};

struct BacktestReport {
  std::string strategy;
  int bars = 0;
  int trades = 0;
  double total_return_pct = 0;     // strategy equity, net of commissions
  double buy_hold_return_pct = 0;  // first close -> last close, no costs
  double max_drawdown_pct = 0;     // on the strategy equity curve
  double sharpe = 0;               // per-bar returns, annualized for daily bars
  double win_rate_pct = 0;
  std::vector<TradeRecord> trade_log;
};

// commission_pct is per side (e.g. 0.05 => 0.05% per fill).
inline BacktestReport run_backtest(Strategy& strategy,
                                   const std::vector<Bar>& bars,
                                   double commission_pct = 0.05,
                                   double annualization = 252.0) {
  BacktestReport report;
  report.strategy = strategy.name();
  report.bars = static_cast<int>(bars.size());
  if (bars.size() < 2) return report;

  const double fee = commission_pct / 100.0;
  double equity = 1.0;
  double peak = 1.0;
  bool long_pos = false;
  double entry_price = 0;
  std::int64_t entry_time = 0;
  double prev_close = bars.front().close;

  std::vector<double> bar_returns;
  bar_returns.reserve(bars.size());

  for (const Bar& bar : bars) {
    const double prev_equity = equity;
    if (long_pos && prev_close > 0)
      equity *= bar.close / prev_close;  // mark to market

    const Action action = strategy.on_bar(bar);
    if (action == Action::buy && !long_pos) {
      long_pos = true;
      entry_price = bar.close;
      entry_time = bar.time;
      equity *= 1.0 - fee;
    } else if (action == Action::sell && long_pos) {
      long_pos = false;
      equity *= 1.0 - fee;
      TradeRecord t;
      t.entry_time = entry_time;
      t.exit_time = bar.time;
      t.entry_price = entry_price;
      t.exit_price = bar.close;
      t.return_pct =
          (bar.close / entry_price * (1.0 - fee) * (1.0 - fee) - 1.0) * 100.0;
      report.trade_log.push_back(t);
    }

    if (prev_equity > 0) bar_returns.push_back(equity / prev_equity - 1.0);
    peak = std::max(peak, equity);
    report.max_drawdown_pct =
        std::max(report.max_drawdown_pct, (peak - equity) / peak * 100.0);
    prev_close = bar.close;
  }

  // Close any open position at the final bar for reporting purposes.
  if (long_pos) {
    equity *= 1.0 - fee;
    TradeRecord t;
    t.entry_time = entry_time;
    t.exit_time = bars.back().time;
    t.entry_price = entry_price;
    t.exit_price = bars.back().close;
    t.return_pct =
        (t.exit_price / entry_price * (1.0 - fee) * (1.0 - fee) - 1.0) * 100.0;
    report.trade_log.push_back(t);
  }

  report.trades = static_cast<int>(report.trade_log.size());
  report.total_return_pct = (equity - 1.0) * 100.0;
  report.buy_hold_return_pct =
      (bars.back().close / bars.front().close - 1.0) * 100.0;

  int wins = 0;
  for (const auto& t : report.trade_log)
    if (t.return_pct > 0) ++wins;
  report.win_rate_pct =
      report.trades ? 100.0 * wins / report.trades : 0.0;

  if (bar_returns.size() > 1) {
    double mean = 0;
    for (double r : bar_returns) mean += r;
    mean /= static_cast<double>(bar_returns.size());
    double var = 0;
    for (double r : bar_returns) var += (r - mean) * (r - mean);
    var /= static_cast<double>(bar_returns.size() - 1);
    const double sd = std::sqrt(var);
    if (sd > 0) report.sharpe = mean / sd * std::sqrt(annualization);
  }
  return report;
}

}  // namespace tinvest::algo
