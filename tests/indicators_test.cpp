#include "tinvest/algo/indicators.hpp"

#include <gtest/gtest.h>

#include "tinvest/algo/strategies.hpp"

using namespace tinvest::algo;

TEST(Sma, ExactRollingAverage) {
  Sma sma(3);
  sma.push(1);
  sma.push(2);
  EXPECT_FALSE(sma.ready());
  sma.push(3);
  ASSERT_TRUE(sma.ready());
  EXPECT_DOUBLE_EQ(sma.value(), 2.0);  // (1+2+3)/3
  sma.push(7);
  EXPECT_DOUBLE_EQ(sma.value(), 4.0);  // (2+3+7)/3
}

TEST(Ema, SeedsWithSmaThenSmooths) {
  Ema ema(3);  // k = 0.5
  ema.push(2);
  ema.push(4);
  EXPECT_FALSE(ema.ready());
  ema.push(6);
  ASSERT_TRUE(ema.ready());
  EXPECT_DOUBLE_EQ(ema.value(), 4.0);  // seed = SMA(2,4,6)
  ema.push(8);
  EXPECT_DOUBLE_EQ(ema.value(), 6.0);  // 8*0.5 + 4*0.5
}

TEST(Rsi, ExtremesOnMonotonicSeries) {
  Rsi up(5);
  for (int i = 1; i <= 10; ++i) up.push(i);
  ASSERT_TRUE(up.ready());
  EXPECT_DOUBLE_EQ(up.value(), 100.0);  // no losses

  Rsi down(5);
  for (int i = 10; i >= 1; --i) down.push(i);
  ASSERT_TRUE(down.ready());
  EXPECT_NEAR(down.value(), 0.0, 1e-9);  // no gains
}

TEST(Rsi, BalancedSeriesNearFifty) {
  Rsi rsi(4);
  // alternating +1/-1: average gain == average loss
  double v = 100;
  rsi.push(v);
  for (int i = 0; i < 20; ++i) {
    v += (i % 2 == 0) ? 1.0 : -1.0;
    rsi.push(v);
  }
  ASSERT_TRUE(rsi.ready());
  EXPECT_NEAR(rsi.value(), 50.0, 10.0);
}

TEST(Macd, ZeroOnConstantSeries) {
  Macd macd(3, 6, 3);
  for (int i = 0; i < 30; ++i) macd.push(42.0);
  ASSERT_TRUE(macd.ready());
  EXPECT_NEAR(macd.macd(), 0.0, 1e-12);
  EXPECT_NEAR(macd.histogram(), 0.0, 1e-12);
}

TEST(Bollinger, CollapsesOnConstantAndBoundsData) {
  Bollinger bb(5, 2.0);
  for (int i = 0; i < 5; ++i) bb.push(10.0);
  ASSERT_TRUE(bb.ready());
  EXPECT_DOUBLE_EQ(bb.middle(), 10.0);
  EXPECT_DOUBLE_EQ(bb.upper(), 10.0);
  EXPECT_DOUBLE_EQ(bb.lower(), 10.0);

  Bollinger bb2(4, 2.0);
  bb2.push(1);
  bb2.push(3);
  bb2.push(5);
  bb2.push(7);
  ASSERT_TRUE(bb2.ready());
  EXPECT_DOUBLE_EQ(bb2.middle(), 4.0);
  EXPECT_GT(bb2.upper(), 7.0);   // 2 stddev beyond the extremes here
  EXPECT_LT(bb2.lower(), 1.0);
}

TEST(Donchian, TracksWindowExtremes) {
  Donchian d(3);
  d.push(10, 5);
  d.push(12, 6);
  EXPECT_FALSE(d.ready());
  d.push(11, 4);
  ASSERT_TRUE(d.ready());
  EXPECT_DOUBLE_EQ(d.upper(), 12.0);
  EXPECT_DOUBLE_EQ(d.lower(), 4.0);
  d.push(9, 7);  // 10/5 leaves the window
  EXPECT_DOUBLE_EQ(d.upper(), 12.0);
  EXPECT_DOUBLE_EQ(d.lower(), 4.0);
  d.push(8, 6);  // 12/6 leaves
  EXPECT_DOUBLE_EQ(d.upper(), 11.0);
  EXPECT_DOUBLE_EQ(d.lower(), 4.0);
}

TEST(SmaCrossStrategy, EmitsBuyOnGoldenCross) {
  SmaCross strat(2, 4);
  // Down-trending then sharply up-trending closes force a fast/slow cross.
  const double closes[] = {10, 9, 8, 7, 6, 5, 9, 13, 17, 21};
  bool saw_buy = false;
  for (double c : closes) {
    Bar bar{};
    bar.close = c;
    if (strat.on_bar(bar) == Action::buy) saw_buy = true;
  }
  EXPECT_TRUE(saw_buy);
}

TEST(DonchianStrategy, BuysOnBreakout) {
  DonchianBreakout strat(3, 3);
  const double data[][3] = {  // high, low, close
      {10, 9, 9.5}, {10, 9, 9.5}, {10, 9, 9.5},
      {12, 10, 11.5},  // close 11.5 > prior 3-bar high (10) => breakout
  };
  Action last = Action::hold;
  for (const auto& d : data) {
    Bar bar{};
    bar.high = d[0];
    bar.low = d[1];
    bar.close = d[2];
    last = strat.on_bar(bar);
  }
  EXPECT_EQ(last, Action::buy);
}
