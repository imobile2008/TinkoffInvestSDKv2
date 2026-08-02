#pragma once

// C++20 coroutine support.
//
// Every unary RPC gains a third overload taking `tinvest::use_awaitable`:
//
//   tinvest::Task<int> run(tinvest::Client& client) {
//     auto accounts =
//         co_await client.users().get_accounts({}, tinvest::use_awaitable);
//     co_return accounts ? 0 : 1;
//   }
//   int main() { return tinvest::sync_wait(run(client)); }
//
// The coroutine resumes on a gRPC worker thread after each co_await — the
// same rule as for callbacks applies: do not block it for long.

#include <coroutine>
#include <exception>
#include <functional>
#include <optional>
#include <semaphore>
#include <utility>

#include "tinvest/error.hpp"

namespace tinvest {

struct use_awaitable_t {
  explicit constexpr use_awaitable_t() = default;
};
inline constexpr use_awaitable_t use_awaitable{};

namespace detail {

// Awaiter bridging the callback overload of a unary RPC to co_await.
template <class Resp>
class UnaryAwaiter {
 public:
  using StartFn = std::function<void(Callback<Resp>)>;

  explicit UnaryAwaiter(StartFn start) : start_(std::move(start)) {}

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> handle) {
    auto start = std::move(start_);
    start([this, handle](Result<Resp> result) {
      result_.emplace(std::move(result));
      handle.resume();  // nothing may touch *this after resume
    });
  }

  Result<Resp> await_resume() { return std::move(*result_); }

 private:
  StartFn start_;
  std::optional<Result<Resp>> result_;
};

}  // namespace detail

// Minimal lazy task: start with co_await inside another Task, or drive from
// synchronous code with sync_wait().
template <class T>
class Task {
 public:
  struct promise_type;
  using handle_type = std::coroutine_handle<promise_type>;

  struct promise_type {
    std::optional<T> value;
    std::exception_ptr exception;
    std::coroutine_handle<> continuation;
    std::function<void()> on_complete;  // used by sync_wait

    Task get_return_object() {
      return Task(handle_type::from_promise(*this));
    }
    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
      bool await_ready() const noexcept { return false; }
      std::coroutine_handle<> await_suspend(handle_type h) noexcept {
        auto& p = h.promise();
        if (p.continuation) return p.continuation;
        if (auto done = std::move(p.on_complete)) done();
        return std::noop_coroutine();  // frame may be destroyed past here
      }
      void await_resume() noexcept {}
    };
    FinalAwaiter final_suspend() noexcept { return {}; }

    void return_value(T v) { value.emplace(std::move(v)); }
    void unhandled_exception() { exception = std::current_exception(); }
  };

  explicit Task(handle_type handle) : handle_(handle) {}
  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }
  ~Task() {
    if (handle_) handle_.destroy();
  }

  auto operator co_await() && noexcept {
    struct Awaiter {
      handle_type handle;
      bool await_ready() const noexcept { return false; }
      std::coroutine_handle<> await_suspend(
          std::coroutine_handle<> awaiting) noexcept {
        handle.promise().continuation = awaiting;
        return handle;  // symmetric transfer: start the child task
      }
      T await_resume() {
        auto& p = handle.promise();
        if (p.exception) std::rethrow_exception(p.exception);
        return std::move(*p.value);
      }
    };
    return Awaiter{handle_};
  }

  template <class U>
  friend U sync_wait(Task<U> task);

 private:
  handle_type handle_;
};

// Runs the task to completion, blocking the calling thread.
template <class T>
T sync_wait(Task<T> task) {
  std::binary_semaphore done(0);
  auto& promise = task.handle_.promise();
  promise.on_complete = [&done] { done.release(); };
  task.handle_.resume();
  done.acquire();
  if (promise.exception) std::rethrow_exception(promise.exception);
  return std::move(*promise.value);
}

}  // namespace tinvest
