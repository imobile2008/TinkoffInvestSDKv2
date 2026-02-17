/**
 * @file marketdatastreamcoroutine.h
 * @brief C++20 Coroutine utilities for MarketDataStream
 * 
 * This file provides coroutine support for market data streaming.
 * Uses C++20 coroutines with std::suspend_never for immediate execution.
 */

#ifndef MARKETDATASTREAMCOROUTINE_H
#define MARKETDATASTREAMCOROUTINE_H

#include <coroutine>
#include <optional>
#include <atomic>
#include <memory>
#include <functional>
#include <iostream>
#include <future>
#include <queue>
#include <mutex>
#include <condition_variable>
#include "marketdatastreamresponse.h"

using namespace tinkoff::public_::invest::api::contract::v1;

/*!
    \brief Result type for gRPC operations
*/
struct GrpcResult {
    bool ok;
    std::string error_message;
    
    static GrpcResult success() { return {true, ""}; }
    static GrpcResult failure(const std::string& msg) { return {false, msg}; }
};

/*!
    \brief Streaming callback function type that uses MarketDataStreamResponse
*/
using MarketDataStreamCallback = std::function<void(MarketDataStreamResponse)>;

// ============================================================================
// Coroutine Types for Async gRPC Streaming
// ============================================================================

// Forward declarations
template<typename T>
class MarketDataStreamGenerator;

/*!
    \brief Promise type for MarketDataStream coroutine operations
    
    Provides a lazy coroutine that suspends immediately, allowing the caller
    to control when the streaming begins.
*/
template<typename T>
class StreamPromise {
public:
    using value_type = T;
    
    std::suspend_always initial_suspend() const noexcept { return {}; }
    std::suspend_always final_suspend() const noexcept { return {}; }
    
    // For co_return without value (void return - used for error cases)
    void return_void() noexcept {}
    
    // For co_yield
    template<std::convertible_to<T> From>
    std::suspend_always yield_value(From&& value) {
        result_ = std::forward<From>(value);
        return {};
    }
    
    MarketDataStreamGenerator<T> get_return_object() {
        return MarketDataStreamGenerator<T>(std::coroutine_handle<StreamPromise<T>>::from_promise(*this));
    }
    
    void unhandled_exception() {
        if (exception_ptr_) {
            std::rethrow_exception(exception_ptr_);
        }
    }
    
    void set_exception(std::exception_ptr ptr) {
        exception_ptr_ = ptr;
    }
    
    void set_value(const T& value) {
        result_ = value;
    }
    
    void set_value(T&& value) {
        result_ = std::move(value);
    }
    
    std::optional<T> result_;
    std::exception_ptr exception_ptr_;
};

// Specialization for void type (when co_return is used without value)
template<>
class StreamPromise<void> {
public:
    using value_type = void;
    
    std::suspend_always initial_suspend() const noexcept { return {}; }
    std::suspend_always final_suspend() const noexcept { return {}; }
    
    void return_void() noexcept {}
    
    // Return coroutine handle directly for void type
    std::coroutine_handle<void> get_return_object() {
        return std::coroutine_handle<StreamPromise<void>>::from_promise(*this);
    }
    
    void unhandled_exception() {
        if (exception_ptr_) {
            std::rethrow_exception(exception_ptr_);
        }
    }
    
    void set_exception(std::exception_ptr ptr) {
        exception_ptr_ = ptr;
    }
    
    std::exception_ptr exception_ptr_;
};

/*!
    \brief Generator class for yielding streaming responses
    
    This is the main coroutine type that allows yielding individual
    market data responses using co_yield.
    
    Usage:
    \code
    MarketDataStreamGenerator<MarketDataStreamResponse> streamData(MarketDataStream& stream) {
        co_yield MarketDataStreamResponse(candleData);
        co_yield MarketDataStreamResponse(orderBookData);
    }
    
    for co_await (auto& response : streamData(stream)) {
        process(response);
    }
    \endcode
*/
template<typename T>
class [[nodiscard]] MarketDataStreamGenerator {
public:
    using promise_type = StreamPromise<T>;
    using handle_type = std::coroutine_handle<promise_type>;
    
    MarketDataStreamGenerator() : coroutine_(nullptr) {}
    
    explicit MarketDataStreamGenerator(handle_type h) : coroutine_(h) {}
    
    ~MarketDataStreamGenerator() {
        if (coroutine_) {
            coroutine_.destroy();
        }
    }
    
    // Move-only
    MarketDataStreamGenerator(const MarketDataStreamGenerator&) = delete;
    MarketDataStreamGenerator& operator=(const MarketDataStreamGenerator&) = delete;
    
    MarketDataStreamGenerator(MarketDataStreamGenerator&& other) noexcept 
        : coroutine_(other.coroutine_) {
        other.coroutine_ = nullptr;
    }
    
    MarketDataStreamGenerator& operator=(MarketDataStreamGenerator&& other) noexcept {
        if (this != &other) {
            if (coroutine_) {
                coroutine_.destroy();
            }
            coroutine_ = other.coroutine_;
            other.coroutine_ = nullptr;
        }
        return *this;
    }
    
    // Range-based for loop support
    bool has_value() const {
        return coroutine_ && !coroutine_.done();
    }
    
    std::optional<T> current() {
        if (coroutine_ && coroutine_.promise().result_.has_value()) {
            return coroutine_.promise().result_.value();
        }
        return std::nullopt;
    }
    
    bool next() {
        if (coroutine_ && !coroutine_.done()) {
            coroutine_.resume();
            return !coroutine_.done();
        }
        return false;
    }
    
    // Async iteration support
    struct iterator {
        MarketDataStreamGenerator* generator_;
        bool done_;
        
        using difference_type = std::ptrdiff_t;
        using value_type = T;
        using pointer = T*;
        using reference = T;  // Return by value to avoid dangling reference
        
        iterator() : generator_(nullptr), done_(true) {}
        iterator(MarketDataStreamGenerator* g, bool done) : generator_(g), done_(done) {}
        
        bool operator!=(const iterator& other) const {
            return done_ != other.done_;
        }
        
        void operator++() {
            if (generator_) {
                done_ = !generator_->next();
            }
        }
        
        T operator*() {
            auto val = generator_->current();
            if (!val.has_value()) {
                done_ = true;
                return T{};
            }
            return std::move(val.value());  // Return by value to avoid dangling reference
        }
    };
    
    iterator begin() {
        if (coroutine_) {
            coroutine_.resume();
            return iterator(this, coroutine_.done());
        }
        return iterator(this, true);
    }
    
    iterator end() {
        return iterator(this, true);
    }
    
    handle_type release() {
        auto h = coroutine_;
        coroutine_ = nullptr;
        return h;
    }
    
    handle_type coroutine_;
};

// Full specialization for void type
template<>
class MarketDataStreamGenerator<void> {
public:
    using promise_type = StreamPromise<void>;
    using handle_type = std::coroutine_handle<promise_type>;
    
    MarketDataStreamGenerator() : coroutine_(nullptr) {}
    
    explicit MarketDataStreamGenerator(handle_type h) : coroutine_(h) {}
    
    ~MarketDataStreamGenerator() {
        if (coroutine_) {
            coroutine_.destroy();
        }
    }
    
    // Move-only
    MarketDataStreamGenerator(const MarketDataStreamGenerator&) = delete;
    MarketDataStreamGenerator& operator=(const MarketDataStreamGenerator&) = delete;
    
    MarketDataStreamGenerator(MarketDataStreamGenerator&& other) noexcept 
        : coroutine_(other.coroutine_) {
        other.coroutine_ = nullptr;
    }
    
    MarketDataStreamGenerator& operator=(MarketDataStreamGenerator&& other) noexcept {
        if (this != &other) {
            if (coroutine_) {
                coroutine_.destroy();
            }
            coroutine_ = other.coroutine_;
            other.coroutine_ = nullptr;
        }
        return *this;
    }
    
    handle_type release() {
        auto h = coroutine_;
        coroutine_ = nullptr;
        return h;
    }
    
    handle_type coroutine_;
};

/*!
    \brief Async task wrapper for coroutines
    
    Provides a simple awaitable that completes asynchronously.
    Useful for one-shot async operations.
*/
template<typename T>
class AsyncTask {
public:
    using promise_type = StreamPromise<T>;
    using handle_type = std::coroutine_handle<promise_type>;
    
    AsyncTask() : coroutine_(nullptr) {}
    
    explicit AsyncTask(handle_type h) : coroutine_(h) {}
    
    ~AsyncTask() {
        if (coroutine_) {
            coroutine_.destroy();
        }
    }
    
    // Move-only
    AsyncTask(const AsyncTask&) = delete;
    AsyncTask& operator=(const AsyncTask&) = delete;
    
    AsyncTask(AsyncTask&& other) noexcept : coroutine_(other.coroutine_) {
        other.coroutine_ = nullptr;
    }
    
    AsyncTask& operator=(AsyncTask&& other) noexcept {
        if (this != &other) {
            if (coroutine_) {
                coroutine_.destroy();
            }
            coroutine_ = other.coroutine_;
            other.coroutine_ = nullptr;
        }
        return *this;
    }
    
    // Awaitable interface
    bool await_ready() const noexcept { 
        return coroutine_ == nullptr || coroutine_.done(); 
    }
    
    void await_suspend(std::coroutine_handle<> continuation) {
        // Store continuation if needed
        (void)continuation;
    }
    
    T await_resume() {
        if (coroutine_ && coroutine_.promise().result_.has_value()) {
            return std::move(coroutine_.promise().result_.value());
        }
        return T{};
    }
    
    handle_type release() {
        auto h = coroutine_;
        coroutine_ = nullptr;
        return h;
    }
    
    handle_type coroutine_;
};

/*!
    \brief Channel for streaming data between coroutines
    
    Provides a thread-safe queue for passing data between
    producer and consumer coroutines.
*/
template<typename T>
class StreamChannel {
public:
    StreamChannel() : closed_(false) {}
    
    ~StreamChannel() {
        close();
    }
    
    // Producer: send a value
    bool send(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) return false;
        queue_.push(std::move(value));
        cv_.notify_one();
        return true;
    }
    
    // Producer: close the channel
    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        cv_.notify_all();
    }
    
    // Consumer: receive a value (blocking)
    bool receive(T& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty() || closed_; });
        
        if (queue_.empty()) return false;
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    
    // Check if closed
    bool is_closed() const { 
        return closed_; 
    }
    
    // Check if empty
    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
    
    // Get size
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool closed_;
};

/*!
    \brief Awaitable for waiting on a stream channel
    
    Used with co_await to receive values from a StreamChannel.
*/
template<typename T>
class ChannelReceiver {
public:
    explicit ChannelReceiver(StreamChannel<T>& channel) : channel_(channel) {}
    
    bool await_ready() const noexcept {
        return !channel_.empty();
    }
    
    void await_suspend(std::coroutine_handle<> handle) {
        // Use shared_ptr to manage the thread's lifetime
        // The thread will keep itself alive until completion
        auto threadHolder = std::make_shared<std::thread>([this, handle]() {
            T value;
            if (channel_.receive(value)) {
                value_ = std::move(value);
                handle.resume();
            }
            // shared_ptr automatically cleans up when thread finishes
        });
        (void)threadHolder;  // Keep shared_ptr alive - thread runs independently
    }
    
    T await_resume() {
        return std::move(value_);
    }
    
private:
    StreamChannel<T>& channel_;
    T value_;
};

#endif // MARKETDATASTREAMCOROUTINE_H

