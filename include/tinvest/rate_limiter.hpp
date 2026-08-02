#pragma once

// Client-side predictive rate limiter mirroring the per-minute method-group
// limits of the user's tariff. Inert until configured — either automatically
// from GetUserTariff via Client::load_rate_limits(), or manually via
// set_limit(). Blocking calls wait for the window to roll over; asynchronous
// calls fail fast (see detail/unary.hpp).

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tinvest/pb.hpp"
#include "users.pb.h"

namespace tinvest {

class RateLimiter {
 public:
  using Clock = std::chrono::steady_clock;

  // `window` is one rate-limit period; the API uses one minute. Shorter
  // windows are only meant for tests.
  explicit RateLimiter(std::chrono::milliseconds window = std::chrono::minutes(1))
      : window_(window) {}

  // Rebuilds the method table from the tariff. Limits of 0 are treated as
  // "unlimited" (the API reports 0 for unrestricted groups).
  void configure(const pb::GetUserTariffResponse& tariff) {
    std::lock_guard lk(mu_);
    by_method_.clear();
    for (const auto& ul : tariff.unary_limits()) {
      auto group = std::make_shared<Group>();
      group->limit = ul.limit_per_minute();
      for (const auto& m : ul.methods()) by_method_[normalize(m)] = group;
    }
    cv_.notify_all();
  }

  // Manual override: one shared window for all listed methods.
  void set_limit(const std::vector<std::string>& methods, int per_minute) {
    std::lock_guard lk(mu_);
    auto group = std::make_shared<Group>();
    group->limit = per_minute;
    for (const auto& m : methods) by_method_[normalize(m)] = group;
    cv_.notify_all();
  }

  bool enabled() const {
    std::lock_guard lk(mu_);
    return !by_method_.empty();
  }

  // Blocks until a slot is available. Methods without a configured limit
  // pass through untouched.
  void acquire(const std::string& method) {
    std::unique_lock lk(mu_);
    auto it = by_method_.find(method);
    if (it == by_method_.end()) return;
    const std::shared_ptr<Group> g = it->second;
    for (;;) {
      if (g->limit <= 0) return;
      roll_window(*g);
      if (g->count < g->limit) {
        ++g->count;
        return;
      }
      cv_.wait_until(lk, g->window_start + window_);
    }
  }

  // Non-blocking variant used by asynchronous calls.
  bool try_acquire(const std::string& method) {
    std::lock_guard lk(mu_);
    auto it = by_method_.find(method);
    if (it == by_method_.end()) return true;
    Group& g = *it->second;
    if (g.limit <= 0) return true;
    roll_window(g);
    if (g.count < g.limit) {
      ++g.count;
      return true;
    }
    return false;
  }

 private:
  struct Group {
    int limit = 0;
    int count = 0;
    Clock::time_point window_start{};  // epoch => expired on first use
  };

  void roll_window(Group& g) {
    const auto now = Clock::now();
    if (now - g.window_start >= window_) {
      g.window_start = now;
      g.count = 0;
      cv_.notify_all();
    }
  }

  static std::string normalize(std::string method) {
    if (!method.empty() && method.front() == '/') method.erase(0, 1);
    return method;
  }

  const std::chrono::milliseconds window_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<std::string, std::shared_ptr<Group>> by_method_;
};

}  // namespace tinvest
