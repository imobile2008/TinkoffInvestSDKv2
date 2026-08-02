#pragma once

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/channel_arguments.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "tinvest/call_options.hpp"
#include "tinvest/error.hpp"
#include "tinvest/rate_limiter.hpp"

namespace tinvest {

class UsersService;
class InstrumentsService;
class MarketDataService;
class OperationsService;
class OrdersService;
class StopOrdersService;
class SandboxService;
class SignalService;

inline constexpr char kDefaultEndpoint[] = "invest-public-api.tbank.ru:443";
inline constexpr char kLegacyEndpoint[] = "invest-public-api.tinkoff.ru:443";

// Automatic retry of *synchronous* calls. Rate-limited calls wait for the
// x-ratelimit-reset window; UNAVAILABLE retries use exponential backoff.
// Asynchronous calls are never retried by the SDK.
struct RetryPolicy {
  int max_attempts = 3;                        // total attempts, 1 = no retry
  bool on_rate_limit = true;                   // RESOURCE_EXHAUSTED
  bool on_unavailable = true;                  // transient transport failures
  std::chrono::milliseconds max_wait{65'000};  // cap on a single retry sleep
};

struct Config {
  std::string token;                       // Bearer token (prod or sandbox)
  std::string endpoint = kDefaultEndpoint;
  std::string app_name = "tinvest-cpp";    // sent as x-app-name
  bool use_tls = true;                     // disable only for local mock servers
  // PEM bundle with trusted root CAs. Empty: use the OS trust store
  // (/etc/ssl/certs/ca-certificates.crt) when present, else gRPC defaults.
  // Note: invest-public-api.tbank.ru is signed by the Russian Trusted CA
  // (Ministry of Digital Development), which must be present in the bundle.
  std::string ca_file;
  std::optional<std::chrono::milliseconds> default_timeout;  // per-call deadline
  RetryPolicy retry{};
  // Optional hook to tune channel arguments before the channel is created.
  std::function<void(grpc::ChannelArguments&)> tune_channel;
};

// Entry point of the SDK. Owns a single multiplexed HTTP/2 channel and one
// stub per service. Thread-safe: all services and streams may be used from
// any thread concurrently.
class Client {
 public:
  explicit Client(Config config);
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  UsersService& users() { return *users_; }
  InstrumentsService& instruments() { return *instruments_; }
  MarketDataService& market_data() { return *market_data_; }
  OperationsService& operations() { return *operations_; }
  OrdersService& orders() { return *orders_; }
  StopOrdersService& stop_orders() { return *stop_orders_; }
  SandboxService& sandbox() { return *sandbox_; }
  SignalService& signals() { return *signals_; }

  const Config& config() const { return config_; }
  const std::shared_ptr<grpc::Channel>& channel() const { return channel_; }

  // Client-side predictive rate limiter (inert until configured).
  RateLimiter& rate_limiter() { return rate_limiter_; }

  // Fetches GetUserTariff and configures the rate limiter with the actual
  // per-minute limits of this token's tariff.
  Result<pb::GetUserTariffResponse> load_rate_limits();

  // Applies auth metadata, x-app-name, custom metadata and the deadline.
  void setup_context(grpc::ClientContext& ctx, const CallOptions& opts) const;

 private:
  Config config_;
  std::string auth_header_;
  std::shared_ptr<grpc::Channel> channel_;
  RateLimiter rate_limiter_;
  std::unique_ptr<UsersService> users_;
  std::unique_ptr<InstrumentsService> instruments_;
  std::unique_ptr<MarketDataService> market_data_;
  std::unique_ptr<OperationsService> operations_;
  std::unique_ptr<OrdersService> orders_;
  std::unique_ptr<StopOrdersService> stop_orders_;
  std::unique_ptr<SandboxService> sandbox_;
  std::unique_ptr<SignalService> signals_;
};

}  // namespace tinvest
