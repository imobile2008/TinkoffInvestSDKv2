#include "tinvest/client.hpp"

#include <grpcpp/create_channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include <fstream>
#include <sstream>
#include <utility>

#include "tinvest/services.hpp"

namespace tinvest {

Client::Client(Config config) : config_(std::move(config)) {
  auth_header_ = "Bearer " + config_.token;

  grpc::ChannelArguments args;
  // Detect dead connections fast; the exchange expects long-lived channels.
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30'000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10'000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
  args.SetUserAgentPrefix(config_.app_name);
  if (config_.tune_channel) config_.tune_channel(args);

  std::shared_ptr<grpc::ChannelCredentials> creds;
  if (config_.use_tls) {
    grpc::SslCredentialsOptions ssl;
    std::string ca_path = config_.ca_file;
    if (ca_path.empty()) {
      // Prefer the OS trust store so locally installed CAs (e.g. the Russian
      // Trusted CA required by invest-public-api.tbank.ru) are honoured.
      const char* system_bundle = "/etc/ssl/certs/ca-certificates.crt";
      if (std::ifstream probe{system_bundle}; probe.good())
        ca_path = system_bundle;
    }
    if (!ca_path.empty()) {
      std::ifstream f(ca_path);
      std::stringstream buf;
      buf << f.rdbuf();
      ssl.pem_root_certs = buf.str();
    }
    creds = grpc::SslCredentials(ssl);
  } else {
    creds = grpc::InsecureChannelCredentials();
  }
  channel_ = grpc::CreateCustomChannel(config_.endpoint, creds, args);

  users_ = std::make_unique<UsersService>(*this);
  instruments_ = std::make_unique<InstrumentsService>(*this);
  market_data_ = std::make_unique<MarketDataService>(*this);
  operations_ = std::make_unique<OperationsService>(*this);
  orders_ = std::make_unique<OrdersService>(*this);
  stop_orders_ = std::make_unique<StopOrdersService>(*this);
  sandbox_ = std::make_unique<SandboxService>(*this);
  signals_ = std::make_unique<SignalService>(*this);
}

Client::~Client() = default;

Result<pb::GetUserTariffResponse> Client::load_rate_limits() {
  auto tariff = users_->get_user_tariff();
  if (tariff) rate_limiter_.configure(*tariff);
  return tariff;
}

void Client::setup_context(grpc::ClientContext& ctx,
                           const CallOptions& opts) const {
  ctx.AddMetadata("authorization", auth_header_);
  ctx.AddMetadata("x-app-name", config_.app_name);
  for (const auto& [key, value] : opts.metadata) ctx.AddMetadata(key, value);
  const auto timeout = opts.timeout ? opts.timeout : config_.default_timeout;
  if (timeout) {
    ctx.set_deadline(std::chrono::system_clock::now() + *timeout);
  }
}

}  // namespace tinvest
