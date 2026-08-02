// Lists user accounts and portfolio totals.
// Usage: TINVEST_TOKEN=t.xxx ./accounts

#include <cstdio>
#include <cstdlib>

#include "tinvest/tinvest.hpp"

int main() {
  const char* token = std::getenv("TINVEST_TOKEN");
  if (!token) {
    std::fprintf(stderr, "Set TINVEST_TOKEN environment variable\n");
    return 1;
  }

  tinvest::Client client({.token = token, .app_name = "tinvest-cpp.examples"});

  auto info = client.users().get_info();
  if (info) {
    std::printf("tariff=%s qualified=%d\n", info->tariff().c_str(),
                static_cast<int>(info->qual_status()));
  } else {
    std::fprintf(stderr, "GetInfo failed: %s\n",
                 info.error().to_string().c_str());
  }

  auto accounts = client.users().get_accounts();
  if (!accounts) {
    std::fprintf(stderr, "GetAccounts failed: %s\n",
                 accounts.error().to_string().c_str());
    return 1;
  }

  for (const auto& acc : accounts->accounts()) {
    std::printf("account %s  name=%s  type=%d  status=%d\n", acc.id().c_str(),
                acc.name().c_str(), static_cast<int>(acc.type()),
                static_cast<int>(acc.status()));

    tinvest::pb::PortfolioRequest preq;
    preq.set_account_id(acc.id());
    auto portfolio = client.operations().get_portfolio(preq);
    if (portfolio) {
      const tinvest::Money total(portfolio->total_amount_portfolio());
      std::printf("  portfolio total: %s, positions: %d\n",
                  total.to_string().c_str(), portfolio->positions_size());
    }
  }
  return 0;
}
