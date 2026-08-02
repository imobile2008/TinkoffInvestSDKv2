// Unary-call tests against an in-process mock gRPC server: metadata,
// error mapping, rate-limit retry, async completion.

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <memory>
#include <string>

#include "tinvest/tinvest.hpp"

namespace pb = tinvest::pb;

namespace {

std::string metadata_value(
    const std::multimap<grpc::string_ref, grpc::string_ref>& md,
    const std::string& key) {
  auto it = md.find(key);
  if (it == md.end()) return {};
  return std::string(it->second.data(), it->second.size());
}

class MockUsers final : public pb::UsersService::Service {
 public:
  grpc::Status GetAccounts(grpc::ServerContext* ctx,
                           const pb::GetAccountsRequest*,
                           pb::GetAccountsResponse* resp) override {
    last_auth_ = metadata_value(ctx->client_metadata(), "authorization");
    last_app_name_ = metadata_value(ctx->client_metadata(), "x-app-name");
    resp->add_accounts()->set_id("acc-1");
    return grpc::Status::OK;
  }

  grpc::Status GetMarginAttributes(
      grpc::ServerContext* ctx, const pb::GetMarginAttributesRequest*,
      pb::GetMarginAttributesResponse*) override {
    ctx->AddTrailingMetadata("message", "Account margin status not found");
    ctx->AddTrailingMetadata("x-tracking-id", "trk-42");
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "30042");
  }

  grpc::Status GetInfo(grpc::ServerContext* ctx, const pb::GetInfoRequest*,
                       pb::GetInfoResponse* resp) override {
    if (info_calls_.fetch_add(1) == 0) {
      ctx->AddTrailingMetadata("x-ratelimit-reset", "0");
      ctx->AddTrailingMetadata("message", "Request limit exceeded");
      return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "80002");
    }
    resp->set_tariff("mock");
    return grpc::Status::OK;
  }

  std::string last_auth_;
  std::string last_app_name_;
  std::atomic<int> info_calls_{0};
};

class ClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&users_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port, 0);

    tinvest::Config cfg;
    cfg.token = "test-token";
    cfg.endpoint = "127.0.0.1:" + std::to_string(port);
    cfg.app_name = "tinvest-tests";
    cfg.use_tls = false;
    client_ = std::make_unique<tinvest::Client>(std::move(cfg));
  }

  void TearDown() override {
    client_.reset();
    server_->Shutdown();
  }

  MockUsers users_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<tinvest::Client> client_;
};

TEST_F(ClientTest, SyncCallReturnsDataAndSendsMetadata) {
  auto r = client_->users().get_accounts();
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  ASSERT_EQ(r->accounts_size(), 1);
  EXPECT_EQ(r->accounts(0).id(), "acc-1");
  EXPECT_EQ(users_.last_auth_, "Bearer test-token");
  EXPECT_EQ(users_.last_app_name_, "tinvest-tests");
}

TEST_F(ClientTest, ErrorMappingFromTrailingMetadata) {
  auto r = client_->users().get_margin_attributes();
  ASSERT_FALSE(r.ok());
  const auto& e = r.error();
  EXPECT_EQ(e.code, grpc::StatusCode::NOT_FOUND);
  EXPECT_EQ(e.api_code, "30042");
  EXPECT_EQ(e.message, "Account margin status not found");
  EXPECT_EQ(e.tracking_id, "trk-42");
}

TEST_F(ClientTest, RateLimitedCallIsRetried) {
  auto r = client_->users().get_info();
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  EXPECT_EQ(r->tariff(), "mock");
  EXPECT_EQ(users_.info_calls_.load(), 2);
}

TEST_F(ClientTest, RetryCanBeDisabledPerCall) {
  tinvest::CallOptions opts;
  opts.retry = false;
  auto r = client_->users().get_info({}, opts);
  ASSERT_FALSE(r.ok());
  EXPECT_TRUE(r.error().rate_limited());
  EXPECT_EQ(r.error().ratelimit.reset_seconds, 0);
  EXPECT_EQ(users_.info_calls_.load(), 1);
}

tinvest::Task<tinvest::Result<pb::GetAccountsResponse>> fetch_accounts(
    tinvest::Client& client) {
  co_return co_await client.users().get_accounts({}, tinvest::use_awaitable);
}

TEST_F(ClientTest, AwaitableCallCompletes) {
  auto r = tinvest::sync_wait(fetch_accounts(*client_));
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  EXPECT_EQ(r->accounts(0).id(), "acc-1");
}

TEST_F(ClientTest, ClientSideRateLimitFailsFastOnAsync) {
  client_->rate_limiter().set_limit(
      {"tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"}, 1);
  auto first = client_->users().get_accounts();
  ASSERT_TRUE(first.ok());

  std::promise<tinvest::Result<pb::GetAccountsResponse>> done;
  auto fut = done.get_future();
  client_->users().get_accounts(
      pb::GetAccountsRequest{},
      [&](tinvest::Result<pb::GetAccountsResponse> r) {
        done.set_value(std::move(r));
      });
  auto r = fut.get();  // limiter rejects synchronously
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.error().code, grpc::StatusCode::RESOURCE_EXHAUSTED);
  EXPECT_EQ(r.error().api_code, "client");
}

TEST_F(ClientTest, AsyncCallCompletes) {
  std::promise<tinvest::Result<pb::GetAccountsResponse>> done;
  auto fut = done.get_future();
  client_->users().get_accounts(
      pb::GetAccountsRequest{},
      [&](tinvest::Result<pb::GetAccountsResponse> r) {
        done.set_value(std::move(r));
      });
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto r = fut.get();
  ASSERT_TRUE(r.ok()) << r.error().to_string();
  EXPECT_EQ(r->accounts(0).id(), "acc-1");
}

}  // namespace
