#include "tinvest/algo/backtest.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "tinvest/algo/strategies.hpp"

using namespace tinvest::algo;

namespace {

std::vector<Bar> make_bars(std::initializer_list<double> closes) {
  std::vector<Bar> bars;
  std::int64_t t = 1'700'000'000;
  for (double c : closes) {
    Bar b{};
    b.time = t;
    b.open = b.high = b.low = b.close = c;
    bars.push_back(b);
    t += 86'400;
  }
  return bars;
}

// Deterministic scripted strategy for exact-math checks.
class Scripted : public Strategy {
 public:
  explicit Scripted(std::vector<Action> actions)
      : actions_(std::move(actions)) {}
  Action on_bar(const Bar&) override {
    return i_ < actions_.size() ? actions_[i_++] : Action::hold;
  }
  std::string name() const override { return "scripted"; }

 private:
  std::vector<Action> actions_;
  std::size_t i_ = 0;
};

TEST(Backtest, ExactPnlWithoutCommission) {
  auto bars = make_bars({100, 100, 110, 121, 121});
  // buy at bar1 close (100), sell at bar3 close (121) => +21%
  Scripted s({Action::hold, Action::buy, Action::hold, Action::sell,
              Action::hold});
  auto r = run_backtest(s, bars, /*commission_pct=*/0.0);
  EXPECT_EQ(r.trades, 1);
  EXPECT_NEAR(r.total_return_pct, 21.0, 1e-9);
  EXPECT_NEAR(r.trade_log[0].return_pct, 21.0, 1e-9);
  EXPECT_NEAR(r.buy_hold_return_pct, 21.0, 1e-9);
  EXPECT_NEAR(r.win_rate_pct, 100.0, 1e-9);
}

TEST(Backtest, CommissionReducesPnl) {
  auto bars = make_bars({100, 100, 110});
  Scripted s({Action::hold, Action::buy, Action::sell});
  auto r = run_backtest(s, bars, /*commission_pct=*/1.0);  // 1% per side
  // 1.10 * 0.99 * 0.99 - 1 = 7.811%
  EXPECT_EQ(r.trades, 1);
  EXPECT_NEAR(r.total_return_pct, 7.811, 1e-3);
}

TEST(Backtest, OpenPositionClosedAtEnd) {
  auto bars = make_bars({100, 100, 150});
  Scripted s({Action::hold, Action::buy, Action::hold});
  auto r = run_backtest(s, bars, 0.0);
  EXPECT_EQ(r.trades, 1);  // force-closed on the last bar
  EXPECT_NEAR(r.total_return_pct, 50.0, 1e-9);
}

TEST(Backtest, MaxDrawdownOnRoundTrip) {
  auto bars = make_bars({100, 100, 200, 100, 100});
  Scripted s({Action::buy, Action::hold, Action::hold, Action::hold,
              Action::sell});
  auto r = run_backtest(s, bars, 0.0);
  EXPECT_NEAR(r.max_drawdown_pct, 50.0, 1e-9);  // 200 -> 100 while long
  EXPECT_NEAR(r.total_return_pct, 0.0, 1e-9);
}

TEST(Backtest, FlatStrategyHasNoTradesOrDrawdown) {
  auto bars = make_bars({100, 90, 80, 70});
  Scripted s({});
  auto r = run_backtest(s, bars, 0.0);
  EXPECT_EQ(r.trades, 0);
  EXPECT_NEAR(r.total_return_pct, 0.0, 1e-9);
  EXPECT_NEAR(r.max_drawdown_pct, 0.0, 1e-9);
  EXPECT_LT(r.buy_hold_return_pct, 0.0);
}

TEST(Backtest, RealStrategiesRunOnSyntheticData) {
  // Sine-ish wave: every strategy must at least run without blowing up.
  std::vector<Bar> bars;
  std::int64_t t = 1'700'000'000;
  for (int i = 0; i < 300; ++i) {
    const double base = 100.0 + 10.0 * std::sin(i / 10.0) + i * 0.05;
    Bar b{};
    b.time = t;
    b.open = base;
    b.high = base + 1;
    b.low = base - 1;
    b.close = base + 0.3;
    bars.push_back(b);
    t += 86'400;
  }
  std::vector<std::unique_ptr<Strategy>> strategies;
  strategies.push_back(std::make_unique<SmaCross>());
  strategies.push_back(std::make_unique<EmaCross>());
  strategies.push_back(std::make_unique<RsiReversion>());
  strategies.push_back(std::make_unique<BollingerReversion>());
  strategies.push_back(std::make_unique<MacdCross>());
  strategies.push_back(std::make_unique<DonchianBreakout>());
  for (auto& s : strategies) {
    auto r = run_backtest(*s, bars);
    EXPECT_EQ(r.bars, 300) << r.strategy;
    EXPECT_GE(r.max_drawdown_pct, 0.0) << r.strategy;
    // A sane long-only result stays within [-100%, +10x] on this data.
    EXPECT_GT(r.total_return_pct, -100.0) << r.strategy;
    EXPECT_LT(r.total_return_pct, 1000.0) << r.strategy;
  }
}

}  // namespace
