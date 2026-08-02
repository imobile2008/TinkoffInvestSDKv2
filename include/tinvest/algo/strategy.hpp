#pragma once

// Strategy interface for the classic signal-driven algorithms: a strategy
// consumes bars one by one and emits buy/sell/hold. Long-only semantics:
// `buy` opens (or keeps) a long position, `sell` closes it.

#include <cstdint>
#include <string>

#include "marketdata.pb.h"
#include "tinvest/decimal.hpp"

namespace tinvest::algo {

struct Bar {
  std::int64_t time = 0;  // unix seconds, UTC
  double open = 0;
  double high = 0;
  double low = 0;
  double close = 0;
  double volume = 0;
};

inline Bar to_bar(const pb::HistoricCandle& c) {
  return Bar{c.time().seconds(),
             Decimal(c.open()).to_double(),
             Decimal(c.high()).to_double(),
             Decimal(c.low()).to_double(),
             Decimal(c.close()).to_double(),
             static_cast<double>(c.volume())};
}

enum class Action { hold, buy, sell };

class Strategy {
 public:
  virtual ~Strategy() = default;
  virtual Action on_bar(const Bar& bar) = 0;
  virtual std::string name() const = 0;
};

}  // namespace tinvest::algo
