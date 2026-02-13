# TinkoffInvestSDKv2 Tests

This directory contains comprehensive tests for the Tinkoff Invest SDK.

## Test Files

| File | Description |
|------|-------------|
| `test_api_integration.cpp` | Unit tests for all API services with mocked responses |
| `test_streaming.cpp` | Tests for streaming services (MarketDataStream, OrdersStream) |
| `test_streaming_last_price.cpp` | **Last price streaming tests - subscribe to 5 instruments for 1 minute** |
| `test_integration_real.cpp` | Real API integration tests (requires valid token) |

## Test Coverage

### 1. Unit Tests (`test_api_integration.cpp`)

- **Common Types Tests**: ServiceReply wrapper class validation
- **InvestApiClient Tests**: Client initialization and service retrieval
- **Sandbox Service Tests**: All sandbox account operations
- **MarketData Service Tests**: Market data queries (candles, prices, orderbook)
- **Users Service Tests**: User account and margin information
- **Instruments Service Tests**: Instrument listings and queries
- **Operations Service Tests**: Portfolio and operations
- **Orders Service Tests**: Trading orders
- **StopOrders Service Tests**: Stop-loss orders
- **Error Handling Tests**: Invalid input handling
- **Edge Cases Tests**: Boundary conditions

### 2. Streaming Tests (`test_streaming.cpp`)

- **MarketDataStream Tests**: Candles, orderbook, trades, info, last prices
- **OrdersStream Tests**: Trades streaming
- **Integration Tests**: Both streaming services working together
- **Performance Tests**: Rapid subscriptions, large subscription lists
- **Error Handling Tests**: Invalid subscriptions

### 3. Integration Tests (`test_integration_real.cpp`)

These tests make actual API calls to Tinkoff Invest API:

- **Sandbox Integration Tests**: Open/close accounts, deposit funds, place orders
- **MarketData Integration Tests**: Real market data queries
- **Users Integration Tests**: Real user information
- **Instruments Integration Tests**: Real instrument listings
- **Full Workflow Test**: Complete trading workflow

## Prerequisites

1. **Build the SDK first**:
   ```bash
   mkdir build && cd build
   cmake ..
   cmake --build .
   ```

2. **Install Google Test**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libgtest-dev cmake
   
   # macOS
   brew install googletest
   
   # Or use vcpkg
   vcpkg install gtest
   ```

3. **Configure API Token** (required for streaming tests):
   See [API_TOKEN_SETUP.md](API_TOKEN_SETUP.md) for detailed instructions.
   
   Quick setup:
   ```bash
   cp .test_token.txt.template .test_token.txt
   # Edit .test_token.txt and add your token
   ```

## Building Tests

```bash
cd build
cmake ..
cmake --build .
```

## Running Tests

### Run all tests:
```bash
cd build
ctest
```

### Run specific test suite:
```bash
./tests/TinkoffInvestSDKTests
```

### Run unit tests only (mocked):
```bash
./tests/TinkoffInvestSDKTests --gtest_filter="-*IntegrationTest*:*Real*"
```

### Run integration tests:
```bash
# Set your Tinkoff token first
export TINKOFF_TOKEN="your_token_here"

./tests/TinkoffInvestSDKTests --gtest_filter="*Integration*"
```

## Environment Variables

| Variable | Required For | Description |
|----------|--------------|-------------|
| `TINKOFF_TOKEN` | Integration tests | Your Tinkoff Invest API token (alternative to file) |

## API Token Configuration

For streaming tests, you can provide your API token either via:

1. **Token file** (`.test_token.txt`) - See [API_TOKEN_SETUP.md](API_TOKEN_SETUP.md)
2. **Environment variable** (`TINKOFF_TOKEN`)

The token file takes precedence over the environment variable.

## Getting a Tinkoff Invest API Token

1. Log in to your Tinkoff Invest account
2. Go to Settings → API
3. Generate a new token
4. Set the `TINKOFF_TOKEN` environment variable

## Test Categories

### Mock-based Tests (Fast, No Network)
- All tests in `test_api_integration.cpp`
- All tests in `test_streaming.cpp`
- Use mocked gRPC responses
- Run in milliseconds

### Integration Tests (Requires Network)
- Tests in `test_integration_real.cpp`
- Make real API calls
- Require valid token
- May take seconds to complete

## Adding New Tests

### Adding a Unit Test

```cpp
TEST_F(ServiceNameTest, TestName) {
    auto reply = service->methodName(params);
    EXPECT_TRUE(reply.GetStatus().ok());
}
```

### Adding an Integration Test

```cpp
TEST_F(ServiceIntegrationTest, TestName) {
    if (!isTokenValid()) {
        GTEST_SKIP() << "Token not set";
    }
    // Test implementation
}
```

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build SDK
        run: |
          mkdir build && cd build
          cmake ..
          cmake --build .
      - name: Run Unit Tests
        run: |
          cd build
          ./tests/TinkoffInvestSDKTests --gtest_filter="-*Integration*"
      - name: Run Integration Tests
        if: env.TINKOFF_TOKEN != ''
        run: |
          cd build
          export TINKOFF_TOKEN=${{ secrets.TINKOFF_TOKEN }}
          ./tests/TinkoffInvestSDKTests --gtest_filter="*Integration*"
```

## Troubleshooting

### CMake can't find GoogleTest
```bash
# Find GTest installation
find_package(GTest REQUIRED)

# If using vcpkg
cmake -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake ..
```

### Linker errors
Ensure you're linking against:
- `GTest::GTest`
- `GTest::Main`
- `Threads::Threads`

### gRPC connection errors
Integration tests require network access to `invest-public-api.tinkoff.ru:443`.
Check your firewall and token permissions.

## Best Practices

1. **Mock-based tests for development**: Fast iteration without API limits
2. **Integration tests before PRs**: Catch real API changes
3. **Token rotation**: Use sandbox tokens for testing
4. **Error handling**: Test both success and failure cases
5. **Coverage**: Aim for >80% code coverage

