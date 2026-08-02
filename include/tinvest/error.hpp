#pragma once

#include <grpcpp/support/status.h>

#include <functional>
#include <string>
#include <utility>
#include <variant>

namespace tinvest {

// Rate-limit metadata returned by the API (x-ratelimit-* headers), -1 if absent.
struct RateLimitInfo {
  int limit = -1;
  int remaining = -1;
  int reset_seconds = -1;
};

struct Error {
  grpc::StatusCode code = grpc::StatusCode::UNKNOWN;
  std::string message;      // human-readable description (trailing metadata "message")
  std::string api_code;     // numeric T-Invest error code, e.g. "30042", if provided
  std::string tracking_id;  // x-tracking-id for support requests
  RateLimitInfo ratelimit{};

  bool rate_limited() const { return code == grpc::StatusCode::RESOURCE_EXHAUSTED; }

  std::string to_string() const {
    std::string s = "grpc=" + std::to_string(static_cast<int>(code));
    if (!api_code.empty()) s += " api=" + api_code;
    if (!message.empty()) s += " " + message;
    if (!tracking_id.empty()) s += " [x-tracking-id: " + tracking_id + "]";
    return s;
  }
};

// Value-or-error result of an API call. No exceptions are thrown by the SDK.
template <class T>
class [[nodiscard]] Result {
 public:
  Result(T value) : data_(std::move(value)) {}
  Result(Error error) : data_(std::move(error)) {}

  bool ok() const { return data_.index() == 0; }
  explicit operator bool() const { return ok(); }

  T& value() & { return std::get<0>(data_); }
  const T& value() const& { return std::get<0>(data_); }
  T&& value() && { return std::get<0>(std::move(data_)); }
  T& operator*() & { return value(); }
  const T& operator*() const& { return value(); }
  T* operator->() { return &value(); }
  const T* operator->() const { return &value(); }

  Error& error() { return std::get<1>(data_); }
  const Error& error() const { return std::get<1>(data_); }

  // Returns value or a default when the call failed.
  T value_or(T fallback) const& { return ok() ? value() : std::move(fallback); }

 private:
  std::variant<T, Error> data_;
};

// Completion callback of asynchronous calls. Invoked on a gRPC worker thread:
// keep it short and non-blocking, hand heavy work to your own executor.
template <class T>
using Callback = std::function<void(Result<T>)>;

}  // namespace tinvest
