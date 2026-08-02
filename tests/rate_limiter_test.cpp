#include "tinvest/rate_limiter.hpp"

#include <gtest/gtest.h>

#include <chrono>

namespace pb = tinvest::pb;
using tinvest::RateLimiter;
using namespace std::chrono_literals;

namespace {

constexpr char kMethod[] =
    "tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts";

TEST(RateLimiter, UnconfiguredPassesEverything) {
  RateLimiter rl;
  EXPECT_FALSE(rl.enabled());
  EXPECT_TRUE(rl.try_acquire(kMethod));
  rl.acquire(kMethod);  // must not block
}

TEST(RateLimiter, TryAcquireHonoursLimit) {
  RateLimiter rl(200ms);
  rl.set_limit({kMethod}, 2);
  EXPECT_TRUE(rl.enabled());
  EXPECT_TRUE(rl.try_acquire(kMethod));
  EXPECT_TRUE(rl.try_acquire(kMethod));
  EXPECT_FALSE(rl.try_acquire(kMethod));
  EXPECT_TRUE(rl.try_acquire("unknown/Method"));  // unlimited
}

TEST(RateLimiter, AcquireBlocksUntilWindowRollsOver) {
  RateLimiter rl(200ms);
  rl.set_limit({kMethod}, 1);
  rl.acquire(kMethod);
  const auto start = std::chrono::steady_clock::now();
  rl.acquire(kMethod);  // must wait for the next window
  const auto waited = std::chrono::steady_clock::now() - start;
  EXPECT_GE(waited, 100ms);
  EXPECT_LT(waited, 2s);
}

TEST(RateLimiter, ConfigureFromTariffResponse) {
  pb::GetUserTariffResponse tariff;
  auto* ul = tariff.add_unary_limits();
  ul->set_limit_per_minute(1);
  // The API reports method names with a leading slash — must be normalized.
  ul->add_methods(std::string("/") + kMethod);
  auto* unlimited = tariff.add_unary_limits();
  unlimited->set_limit_per_minute(0);  // 0 => unlimited
  unlimited->add_methods("tinkoff.public.invest.api.contract.v1.UsersService/GetInfo");

  RateLimiter rl(200ms);
  rl.configure(tariff);
  EXPECT_TRUE(rl.try_acquire(kMethod));
  EXPECT_FALSE(rl.try_acquire(kMethod));
  EXPECT_TRUE(rl.try_acquire(
      "tinkoff.public.invest.api.contract.v1.UsersService/GetInfo"));
}

TEST(RateLimiter, GroupsShareOneWindow) {
  RateLimiter rl(200ms);
  rl.set_limit({"svc/A", "svc/B"}, 2);
  EXPECT_TRUE(rl.try_acquire("svc/A"));
  EXPECT_TRUE(rl.try_acquire("svc/B"));
  EXPECT_FALSE(rl.try_acquire("svc/A"));  // group quota shared across methods
}

}  // namespace
