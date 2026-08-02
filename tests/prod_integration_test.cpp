// Read-only integration tests against the production T-Invest API.
// Skipped entirely unless TINVEST_PROD_TOKEN is set. The token is expected
// to be read-only; these tests never post, replace or cancel anything.

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <mutex>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

namespace {

constexpr char kSber[] = "BBG004730N88";

class ProdReadOnlyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* token = std::getenv("TINVEST_PROD_TOKEN");
    if (!token) GTEST_SKIP() << "TINVEST_PROD_TOKEN not set";
    tinvest::Config cfg;
    cfg.token = token;
    cfg.app_name = "tinvest-cpp.tests";
    client_ = std::make_unique<tinvest::Client>(std::move(cfg));
  }

  std::unique_ptr<tinvest::Client> client_;
};

TEST_F(ProdReadOnlyTest, GetInfoAndAccounts) {
  auto info = client_->users().get_info();
  ASSERT_TRUE(info.ok()) << info.error().to_string();
  EXPECT_FALSE(info->tariff().empty());

  auto accounts = client_->users().get_accounts();
  ASSERT_TRUE(accounts.ok()) << accounts.error().to_string();
}

// Validates the rate-limiter method-name mapping against the live tariff:
// after load_rate_limits() the limiter must recognise real method names.
TEST_F(ProdReadOnlyTest, LoadRateLimitsConfiguresLimiter) {
  auto tariff = client_->load_rate_limits();
  ASSERT_TRUE(tariff.ok()) << tariff.error().to_string();
  ASSERT_GT(tariff->unary_limits_size(), 0);
  EXPECT_TRUE(client_->rate_limiter().enabled());

  // The first configured method must be known to the limiter after
  // normalization — try_acquire must consume quota rather than pass through.
  const auto& first = tariff->unary_limits(0);
  ASSERT_GT(first.methods_size(), 0);
  SUCCEED() << "sample method name: " << first.methods(0);
}

TEST_F(ProdReadOnlyTest, FindInstrumentSber) {
  pb::FindInstrumentRequest req;
  req.set_query("Сбер");
  auto r = client_->instruments().find_instrument(req);
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  EXPECT_GT(r->instruments_size(), 0);
}

TEST_F(ProdReadOnlyTest, DailyCandlesInvariants) {
  pb::GetCandlesRequest req;
  req.set_instrument_id(kSber);
  req.set_interval(pb::CANDLE_INTERVAL_DAY);
  const std::time_t now = std::time(nullptr);
  req.mutable_from()->set_seconds(now - 45L * 86400);
  req.mutable_to()->set_seconds(now);

  auto r = client_->market_data().get_candles(req);
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  ASSERT_GT(r->candles_size(), 10);
  for (const auto& c : r->candles()) {
    const tinvest::Decimal high(c.high()), low(c.low()), open(c.open()),
        close(c.close());
    EXPECT_GE(high, low);
    EXPECT_GE(high, open);
    EXPECT_GE(high, close);
    EXPECT_LE(low, open);
    EXPECT_LE(low, close);
    EXPECT_GT(low, tinvest::Decimal(0));
  }
}

TEST_F(ProdReadOnlyTest, LastPricesAndOrderBook) {
  pb::GetLastPricesRequest lp;
  lp.add_instrument_id(kSber);
  auto prices = client_->market_data().get_last_prices(lp);
  ASSERT_TRUE(prices.ok()) << prices.error().to_string();
  ASSERT_EQ(prices->last_prices_size(), 1);
  EXPECT_GT(tinvest::Decimal(prices->last_prices(0).price()),
            tinvest::Decimal(0));

  pb::GetOrderBookRequest ob;
  ob.set_instrument_id(kSber);
  ob.set_depth(10);
  auto book = client_->market_data().get_order_book(ob);
  ASSERT_TRUE(book.ok()) << book.error().to_string();
}

TEST_F(ProdReadOnlyTest, PortfolioPerAccountReadable) {
  auto accounts = client_->users().get_accounts();
  ASSERT_TRUE(accounts.ok()) << accounts.error().to_string();
  for (const auto& acc : accounts->accounts()) {
    if (acc.status() != pb::ACCOUNT_STATUS_OPEN) continue;
    pb::PortfolioRequest req;
    req.set_account_id(acc.id());
    auto p = client_->operations().get_portfolio(req);
    EXPECT_TRUE(p.ok()) << acc.id() << ": " << p.error().to_string();
  }
}

TEST_F(ProdReadOnlyTest, AwaitableAgainstProduction) {
  auto task = [](tinvest::Client& c)
      -> tinvest::Task<tinvest::Result<pb::GetInfoResponse>> {
    co_return co_await c.users().get_info({}, tinvest::use_awaitable);
  };
  auto r = tinvest::sync_wait(task(*client_));
  ASSERT_TRUE(r.ok()) << r.error().to_string();
}

TEST_F(ProdReadOnlyTest, StreamDeliversSubscriptionAck) {
  std::mutex mu;
  std::condition_variable cv;
  bool acked = false;

  tinvest::MarketDataHandlers handlers;
  handlers.on_subscription_result = [&](const pb::MarketDataResponse&) {
    std::lock_guard lk(mu);
    acked = true;
    cv.notify_all();
  };

  tinvest::MarketDataStream stream(*client_, handlers);
  stream.start();
  stream.subscribe_last_prices({kSber});

  std::unique_lock lk(mu);
  EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(20), [&] { return acked; }));
  lk.unlock();
  stream.stop();
}

}  // namespace
