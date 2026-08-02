#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tinvest {

// Per-call overrides. Everything is optional; client defaults apply otherwise.
struct CallOptions {
  // Deadline for this call; overrides Config::default_timeout.
  std::optional<std::chrono::milliseconds> timeout;
  // Extra metadata pairs appended to the request (keys must be lowercase).
  std::vector<std::pair<std::string, std::string>> metadata;
  // Force-enable/disable automatic retry for this call (sync calls only).
  std::optional<bool> retry;
};

}  // namespace tinvest
