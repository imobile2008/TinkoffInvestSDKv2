// Full sandbox round-trip: open account -> pay in -> market buy ->
// portfolio -> close account.
// Usage: TINVEST_TOKEN=t.sandbox_xxx ./sandbox_trade [instrument_id]
// Default instrument: BBG004730N88 (SBER). Requires a SANDBOX token.

#include <cstdio>
#include <cstdlib>
#include <string>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

int main(int argc, char** argv) {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN (sandbox token)\n");
    return 1;
  }
  const std::string instrument = argc > 1 ? argv[1] : "BBG004730N88";

  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});
  auto& sb = client.sandbox();

  auto opened = sb.open_sandbox_account();
  if (!opened) {
    std::fprintf(stderr, "OpenSandboxAccount failed: %s\n",
                 opened.error().to_string().c_str());
    return 1;
  }
  const std::string account_id = opened->account_id();
  std::printf("opened sandbox account %s\n", account_id.c_str());

  pb::SandboxPayInRequest pay;
  pay.set_account_id(account_id);
  *pay.mutable_amount() =
      tinvest::Decimal(100'000).to_money("rub");
  if (auto r = sb.sandbox_pay_in(pay); r) {
    std::printf("balance: %s\n", tinvest::Money(r->balance()).to_string().c_str());
  } else {
    std::fprintf(stderr, "SandboxPayIn failed: %s\n",
                 r.error().to_string().c_str());
  }

  pb::PostOrderRequest order;
  order.set_account_id(account_id);
  order.set_instrument_id(instrument);
  order.set_quantity(1);
  order.set_direction(pb::ORDER_DIRECTION_BUY);
  order.set_order_type(pb::ORDER_TYPE_MARKET);
  auto posted = sb.post_sandbox_order(order);
  if (posted) {
    std::printf("order %s status=%d executed=%s\n",
                posted->order_id().c_str(),
                static_cast<int>(posted->execution_report_status()),
                tinvest::Money(posted->executed_order_price()).to_string().c_str());
  } else {
    std::fprintf(stderr, "PostSandboxOrder failed: %s\n",
                 posted.error().to_string().c_str());
  }

  pb::PortfolioRequest preq;
  preq.set_account_id(account_id);
  if (auto p = sb.get_sandbox_portfolio(preq); p) {
    for (const auto& pos : p->positions()) {
      std::printf("position %s x %s\n", pos.figi().c_str(),
                  tinvest::Decimal(pos.quantity()).to_string().c_str());
    }
  }

  pb::CloseSandboxAccountRequest creq;
  creq.set_account_id(account_id);
  if (sb.close_sandbox_account(creq)) {
    std::printf("closed sandbox account\n");
  }
  return 0;
}
