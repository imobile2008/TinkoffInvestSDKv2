#pragma once

// Classic, widely known trading algorithms in their textbook form.
// These are reference implementations for research/backtesting — not advice.

#include <string>

#include "tinvest/algo/indicators.hpp"
#include "tinvest/algo/strategy.hpp"

namespace tinvest::algo {

// Golden/death cross: buy when the fast SMA crosses above the slow one,
// sell on the opposite cross.
class SmaCross : public Strategy {
 public:
  SmaCross(int fast = 20, int slow = 50)
      : fast_(fast), slow_(slow), label_("SMA cross " + std::to_string(fast) +
                                         "/" + std::to_string(slow)) {}
  Action on_bar(const Bar& bar) override {
    fast_.push(bar.close);
    slow_.push(bar.close);
    if (!slow_.ready()) return Action::hold;
    const bool above = fast_.value() > slow_.value();
    Action a = Action::hold;
    if (has_prev_) {
      if (above && !prev_above_) a = Action::buy;
      if (!above && prev_above_) a = Action::sell;
    }
    prev_above_ = above;
    has_prev_ = true;
    return a;
  }
  std::string name() const override { return label_; }

 private:
  Sma fast_, slow_;
  std::string label_;
  bool prev_above_ = false, has_prev_ = false;
};

// EMA crossover (the faster-reacting sibling of SmaCross).
class EmaCross : public Strategy {
 public:
  EmaCross(int fast = 12, int slow = 26)
      : fast_(fast), slow_(slow), label_("EMA cross " + std::to_string(fast) +
                                         "/" + std::to_string(slow)) {}
  Action on_bar(const Bar& bar) override {
    fast_.push(bar.close);
    slow_.push(bar.close);
    if (!slow_.ready() || !fast_.ready()) return Action::hold;
    const bool above = fast_.value() > slow_.value();
    Action a = Action::hold;
    if (has_prev_) {
      if (above && !prev_above_) a = Action::buy;
      if (!above && prev_above_) a = Action::sell;
    }
    prev_above_ = above;
    has_prev_ = true;
    return a;
  }
  std::string name() const override { return label_; }

 private:
  Ema fast_, slow_;
  std::string label_;
  bool prev_above_ = false, has_prev_ = false;
};

// RSI mean reversion: buy oversold (< lower), sell overbought (> upper).
class RsiReversion : public Strategy {
 public:
  RsiReversion(int period = 14, double lower = 30, double upper = 70)
      : rsi_(period), lower_(lower), upper_(upper),
        label_("RSI(" + std::to_string(period) + ") reversion") {}
  Action on_bar(const Bar& bar) override {
    rsi_.push(bar.close);
    if (!rsi_.ready()) return Action::hold;
    const double v = rsi_.value();
    if (v < lower_) return Action::buy;
    if (v > upper_) return Action::sell;
    return Action::hold;
  }
  std::string name() const override { return label_; }

 private:
  Rsi rsi_;
  double lower_, upper_;
  std::string label_;
};

// Bollinger mean reversion: buy a close below the lower band, take profit
// once price reverts above the middle band.
class BollingerReversion : public Strategy {
 public:
  BollingerReversion(int period = 20, double stddev_mult = 2.0)
      : bb_(period, stddev_mult),
        label_("Bollinger(" + std::to_string(period) + ") reversion") {}
  Action on_bar(const Bar& bar) override {
    bb_.push(bar.close);
    if (!bb_.ready()) return Action::hold;
    if (bar.close < bb_.lower()) return Action::buy;
    if (bar.close > bb_.middle()) return Action::sell;
    return Action::hold;
  }
  std::string name() const override { return label_; }

 private:
  Bollinger bb_;
  std::string label_;
};

// MACD signal-line cross: buy when the histogram turns positive, sell when
// it turns negative.
class MacdCross : public Strategy {
 public:
  MacdCross(int fast = 12, int slow = 26, int signal = 9)
      : macd_(fast, slow, signal), label_("MACD " + std::to_string(fast) + "/" +
                                          std::to_string(slow) + "/" +
                                          std::to_string(signal)) {}
  Action on_bar(const Bar& bar) override {
    macd_.push(bar.close);
    if (!macd_.ready()) return Action::hold;
    const bool positive = macd_.histogram() > 0;
    Action a = Action::hold;
    if (has_prev_) {
      if (positive && !prev_positive_) a = Action::buy;
      if (!positive && prev_positive_) a = Action::sell;
    }
    prev_positive_ = positive;
    has_prev_ = true;
    return a;
  }
  std::string name() const override { return label_; }

 private:
  Macd macd_;
  std::string label_;
  bool prev_positive_ = false, has_prev_ = false;
};

// Donchian breakout (the "turtle" rules): buy a close above the previous
// `entry`-bar high, exit on a close below the previous `exit`-bar low.
class DonchianBreakout : public Strategy {
 public:
  DonchianBreakout(int entry = 20, int exit = 10)
      : entry_(entry), exit_(exit),
        label_("Donchian breakout " + std::to_string(entry) + "/" +
               std::to_string(exit)) {}
  Action on_bar(const Bar& bar) override {
    Action a = Action::hold;
    if (entry_.ready() && bar.close > entry_.upper()) a = Action::buy;
    if (exit_.ready() && bar.close < exit_.lower()) a = Action::sell;
    entry_.push(bar.high, bar.low);
    exit_.push(bar.high, bar.low);
    return a;
  }
  std::string name() const override { return label_; }

 private:
  Donchian entry_, exit_;
  std::string label_;
};

}  // namespace tinvest::algo
