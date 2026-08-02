#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <map>
#include <string>
#include <thread>
#include <utility>

#include "tinvest/client.hpp"
#include "tinvest/error.hpp"

namespace tinvest::detail {

inline std::string get_metadata(
    const std::multimap<grpc::string_ref, grpc::string_ref>& md,
    const char* key) {
  auto it = md.find(key);
  if (it == md.end()) return {};
  return std::string(it->second.data(), it->second.size());
}

inline int get_int_metadata(
    const std::multimap<grpc::string_ref, grpc::string_ref>& md,
    const char* key) {
  const std::string v = get_metadata(md, key);
  if (v.empty()) return -1;
  return std::atoi(v.c_str());
}

// Builds Error from a finished call: grpc status + trailing metadata.
inline Error make_error(const grpc::Status& status, grpc::ClientContext& ctx) {
  Error e;
  e.code = status.error_code();
  const auto& md = ctx.GetServerTrailingMetadata();
  e.tracking_id = get_metadata(md, "x-tracking-id");
  e.message = get_metadata(md, "message");
  const std::string& sm = status.error_message();
  const bool numeric =
      !sm.empty() && sm.find_first_not_of("0123456789") == std::string::npos;
  if (numeric) e.api_code = sm;
  if (e.message.empty()) e.message = sm;
  e.ratelimit.limit = get_int_metadata(md, "x-ratelimit-limit");
  e.ratelimit.remaining = get_int_metadata(md, "x-ratelimit-remaining");
  e.ratelimit.reset_seconds = get_int_metadata(md, "x-ratelimit-reset");
  return e;
}

// Blocking unary call with the client's retry policy and rate limiter.
// CallFn: grpc::Status(grpc::ClientContext&, Resp*)
template <class Resp, class CallFn>
Result<Resp> unary_sync(Client& client, const CallOptions& opts,
                        const std::string& method, CallFn&& call) {
  const RetryPolicy& rp = client.config().retry;
  for (int attempt = 1;; ++attempt) {
    client.rate_limiter().acquire(method);
    grpc::ClientContext ctx;
    client.setup_context(ctx, opts);
    Resp resp;
    const grpc::Status status = call(ctx, &resp);
    if (status.ok()) return Result<Resp>(std::move(resp));

    Error err = make_error(status, ctx);
    bool retryable =
        (err.code == grpc::StatusCode::RESOURCE_EXHAUSTED && rp.on_rate_limit) ||
        (err.code == grpc::StatusCode::UNAVAILABLE && rp.on_unavailable);
    if (opts.retry.has_value()) retryable = retryable && *opts.retry;
    if (!retryable || attempt >= std::max(1, rp.max_attempts))
      return Result<Resp>(std::move(err));

    std::chrono::milliseconds delay;
    if (err.code == grpc::StatusCode::RESOURCE_EXHAUSTED) {
      delay = err.ratelimit.reset_seconds >= 0
                  ? std::chrono::seconds(err.ratelimit.reset_seconds) +
                        std::chrono::milliseconds(200)
                  : std::chrono::seconds(attempt);
    } else {
      delay = std::chrono::milliseconds(100 << std::min(attempt - 1, 8));
    }
    if (delay > rp.max_wait) return Result<Resp>(std::move(err));
    std::this_thread::sleep_for(delay);
  }
}

// Non-blocking unary call over the gRPC callback API. No automatic retry.
// If the client-side rate limit is exhausted the callback fires immediately
// (from the calling thread) with a local RESOURCE_EXHAUSTED error,
// api_code = "client".
// StartFn: void(grpc::ClientContext*, const Req*, Resp*,
//               std::function<void(grpc::Status)>)
template <class Req, class Resp, class StartFn>
void unary_async(Client& client, const CallOptions& opts,
                 const std::string& method, Req req, Callback<Resp> cb,
                 StartFn&& start) {
  if (!client.rate_limiter().try_acquire(method)) {
    Error e;
    e.code = grpc::StatusCode::RESOURCE_EXHAUSTED;
    e.message = "client-side rate limit exceeded";
    e.api_code = "client";
    cb(Result<Resp>(std::move(e)));
    return;
  }
  struct State {
    grpc::ClientContext ctx;
    Req req;
    Resp resp;
    Callback<Resp> cb;
    State(Req r, Callback<Resp> c) : req(std::move(r)), cb(std::move(c)) {}
  };
  auto* st = new State(std::move(req), std::move(cb));
  client.setup_context(st->ctx, opts);
  start(&st->ctx, &st->req, &st->resp, [st](grpc::Status status) {
    if (status.ok()) {
      st->cb(Result<Resp>(std::move(st->resp)));
    } else {
      st->cb(Result<Resp>(make_error(status, st->ctx)));
    }
    delete st;
  });
}

}  // namespace tinvest::detail
