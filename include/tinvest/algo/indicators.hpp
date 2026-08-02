#pragma once

// Streaming (incremental) technical indicators: each push() consumes the next
// bar/price in O(1)..O(window) and value() returns the current reading once
// ready(). Doubles are fine here — indicators feed decisions, not accounting.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <deque>
#include <optional>

namespace tinvest::algo {

class Sma {
 public:
  explicit Sma(int period) : period_(period) {}
  void push(double v) {
    window_.push_back(v);
    sum_ += v;
    if (window_.size() > static_cast<std::size_t>(period_)) {
      sum_ -= window_.front();
      window_.pop_front();
    }
  }
  bool ready() const {
    return window_.size() == static_cast<std::size_t>(period_);
  }
  double value() const { return sum_ / static_cast<double>(window_.size()); }

 private:
  int period_;
  std::deque<double> window_;
  double sum_ = 0;
};

// EMA seeded with the SMA of the first `period` values (standard convention).
class Ema {
 public:
  explicit Ema(int period)
      : period_(period), k_(2.0 / (period + 1.0)), seed_(period) {}
  void push(double v) {
    if (!ready_) {
      seed_.push(v);
      if (seed_.ready()) {
        value_ = seed_.value();
        ready_ = true;
      }
      return;
    }
    value_ = v * k_ + value_ * (1.0 - k_);
  }
  bool ready() const { return ready_; }
  double value() const { return value_; }
  int period() const { return period_; }

 private:
  int period_;
  double k_;
  Sma seed_;
  bool ready_ = false;
  double value_ = 0;
};

// Wilder's RSI.
class Rsi {
 public:
  explicit Rsi(int period = 14) : period_(period) {}
  void push(double close) {
    if (!has_prev_) {
      prev_ = close;
      has_prev_ = true;
      return;
    }
    const double delta = close - prev_;
    prev_ = close;
    const double gain = delta > 0 ? delta : 0;
    const double loss = delta < 0 ? -delta : 0;
    if (warmup_ < period_) {
      gain_sum_ += gain;
      loss_sum_ += loss;
      if (++warmup_ == period_) {
        avg_gain_ = gain_sum_ / period_;
        avg_loss_ = loss_sum_ / period_;
        ready_ = true;
      }
      return;
    }
    avg_gain_ = (avg_gain_ * (period_ - 1) + gain) / period_;
    avg_loss_ = (avg_loss_ * (period_ - 1) + loss) / period_;
  }
  bool ready() const { return ready_; }
  double value() const {
    if (avg_loss_ == 0) return 100.0;
    const double rs = avg_gain_ / avg_loss_;
    return 100.0 - 100.0 / (1.0 + rs);
  }

 private:
  int period_;
  double prev_ = 0;
  bool has_prev_ = false;
  int warmup_ = 0;
  double gain_sum_ = 0, loss_sum_ = 0;
  double avg_gain_ = 0, avg_loss_ = 0;
  bool ready_ = false;
};

// MACD(fast, slow) with an EMA signal line over the MACD value.
class Macd {
 public:
  Macd(int fast = 12, int slow = 26, int signal = 9)
      : fast_(fast), slow_(slow), signal_(signal) {}
  void push(double close) {
    fast_.push(close);
    slow_.push(close);
    if (fast_.ready() && slow_.ready())
      signal_.push(fast_.value() - slow_.value());
  }
  bool ready() const { return signal_.ready(); }
  double macd() const { return fast_.value() - slow_.value(); }
  double signal() const { return signal_.value(); }
  double histogram() const { return macd() - signal(); }

 private:
  Ema fast_;
  Ema slow_;
  Ema signal_;
};

// Bollinger Bands: SMA(period) +/- stddev_mult * population stddev.
class Bollinger {
 public:
  explicit Bollinger(int period = 20, double stddev_mult = 2.0)
      : period_(period), mult_(stddev_mult) {}
  void push(double v) {
    window_.push_back(v);
    if (window_.size() > static_cast<std::size_t>(period_))
      window_.pop_front();
  }
  bool ready() const {
    return window_.size() == static_cast<std::size_t>(period_);
  }
  double middle() const {
    double s = 0;
    for (double v : window_) s += v;
    return s / static_cast<double>(window_.size());
  }
  double stddev() const {
    const double m = middle();
    double acc = 0;
    for (double v : window_) acc += (v - m) * (v - m);
    return std::sqrt(acc / static_cast<double>(window_.size()));
  }
  double upper() const { return middle() + mult_ * stddev(); }
  double lower() const { return middle() - mult_ * stddev(); }

 private:
  int period_;
  double mult_;
  std::deque<double> window_;
};

// Donchian channel: highest high / lowest low over the trailing window,
// NOT including the current bar (breakout comparisons need the prior range).
class Donchian {
 public:
  explicit Donchian(int period) : period_(period) {}
  void push(double high, double low) {
    highs_.push_back(high);
    lows_.push_back(low);
    if (highs_.size() > static_cast<std::size_t>(period_)) {
      highs_.pop_front();
      lows_.pop_front();
    }
  }
  bool ready() const {
    return highs_.size() == static_cast<std::size_t>(period_);
  }
  double upper() const {
    return *std::max_element(highs_.begin(), highs_.end());
  }
  double lower() const { return *std::min_element(lows_.begin(), lows_.end()); }

 private:
  int period_;
  std::deque<double> highs_;
  std::deque<double> lows_;
};

}  // namespace tinvest::algo
