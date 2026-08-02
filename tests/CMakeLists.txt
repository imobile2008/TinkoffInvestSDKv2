include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(tinvest_tests
  decimal_test.cpp
  client_test.cpp
  rate_limiter_test.cpp
  stream_test.cpp
  indicators_test.cpp
  backtest_test.cpp
  prod_integration_test.cpp
)
target_link_libraries(tinvest_tests PRIVATE tinvest GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(tinvest_tests PROPERTIES TIMEOUT 60)
