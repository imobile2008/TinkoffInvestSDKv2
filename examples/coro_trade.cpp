// Sandbox round-trip written as a C++20 coroutine with co_await.
// Usage: TINVEST_TOKEN=t.sandbox_xxx ./coro_trade [instrument_id]

#include <cstdio>
#include <cstdlib>
#include <string>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

tinvest::Task<int> run(tinvest::Client& client, std::string instrument) {
  auto& sb = client.sandbox();

  auto opened =
      co_await sb.open_sandbox_account({}, tinvest::use_awaitable);
  if (!opened) {
    std::fprintf(stderr, "OpenSandboxAccount failed: %s\n",
                 opened.error().to_string().c_str());
    co_return 1;
  }
  const std::string account_id = opened->account_id();
  std::printf("opened sandbox account %s\n", account_id.c_str());

  pb::SandboxPayInRequest pay;
  pay.set_account_id(account_id);
  *pay.mutable_amount() = tinvest::Decimal(50'000).to_money("rub");
  auto paid = co_await sb.sandbox_pay_in(std::move(pay), tinvest::use_awaitable);
  if (paid)
    std::printf("balance: %s\n",
                tinvest::Money(paid->balance()).to_string().c_str());

  pb::PostOrderRequest order;
  order.set_account_id(account_id);
  order.set_instrument_id(instrument);
  order.set_quantity(1);
  order.set_direction(pb::ORDER_DIRECTION_BUY);
  order.set_order_type(pb::ORDER_TYPE_MARKET);
  auto posted =
      co_await sb.post_sandbox_order(std::move(order), tinvest::use_awaitable);
  if (posted) {
    std::printf("order %s executed=%s\n", posted->order_id().c_str(),
                tinvest::Money(posted->executed_order_price())
                    .to_string()
                    .c_str());
  } else {
    std::fprintf(stderr, "PostSandboxOrder failed: %s\n",
                 posted.error().to_string().c_str());
  }

  pb::CloseSandboxAccountRequest close;
  close.set_account_id(account_id);
  co_await sb.close_sandbox_account(std::move(close), tinvest::use_awaitable);
  std::printf("closed sandbox account\n");
  co_return 0;
}

int main(int argc, char** argv) {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN (sandbox token)\n");
    return 1;
  }
  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});
  return tinvest::sync_wait(
      run(client, argc > 1 ? argv[1] : "BBG004730N88"));
}
