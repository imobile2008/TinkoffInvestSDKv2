cmake_minimum_required(VERSION 3.22)
project(tinvest VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(TINVEST_BUILD_EXAMPLES "Build example programs" ON)
option(TINVEST_BUILD_TESTS "Build unit/integration tests" ON)
option(TINVEST_BUILD_BENCH "Build benchmark and soak tools" ON)
option(TINVEST_INSTALL "Generate install rules" ON)

# TINVEST_SANITIZE: "" (off), "address" (ASan+UBSan+LSan) or "thread" (TSan).
set(TINVEST_SANITIZE "" CACHE STRING "Sanitizer preset: address or thread")
if(TINVEST_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
elseif(TINVEST_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

include(GNUInstallDirs)
include(cmake/protogen.cmake)

# --- generated protobuf/grpc code -------------------------------------------
set(TINVEST_PROTOS
  contracts/common.proto
  contracts/instruments.proto
  contracts/marketdata.proto
  contracts/operations.proto
  contracts/orders.proto
  contracts/sandbox.proto
  contracts/signals.proto
  contracts/stoporders.proto
  contracts/users.proto
  contracts/google/api/field_behavior.proto
)

tinvest_generate_protos(tinvest_proto "${TINVEST_PROTOS}")

# --- the SDK itself ----------------------------------------------------------
configure_file(cmake/version.hpp.in ${CMAKE_BINARY_DIR}/gen/tinvest/version.hpp)

add_library(tinvest STATIC
  src/client.cpp
  src/market_data_stream.cpp
)
add_library(tinvest::tinvest ALIAS tinvest)
target_include_directories(tinvest PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(tinvest PUBLIC tinvest_proto)

if(TINVEST_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(TINVEST_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(TINVEST_BUILD_BENCH)
  add_subdirectory(bench)
endif()

# --- install & export --------------------------------------------------------
if(TINVEST_INSTALL)
  include(CMakePackageConfigHelpers)

  install(TARGETS tinvest tinvest_proto EXPORT tinvestTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
  install(DIRECTORY include/tinvest
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  )
  # Generated protobuf/grpc headers plus version.hpp; consumers get this
  # directory on their include path via the exported tinvest_proto target.
  install(DIRECTORY ${CMAKE_BINARY_DIR}/gen/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tinvest-proto
    FILES_MATCHING PATTERN "*.pb.h" PATTERN "version.hpp"
  )
  install(FILES ${CMAKE_BINARY_DIR}/gen/tinvest/version.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tinvest
  )

  install(EXPORT tinvestTargets
    NAMESPACE tinvest::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tinvest
  )
  configure_package_config_file(cmake/tinvestConfig.cmake.in
    ${CMAKE_BINARY_DIR}/tinvestConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tinvest
  )
  write_basic_package_version_file(
    ${CMAKE_BINARY_DIR}/tinvestConfigVersion.cmake
    COMPATIBILITY SameMajorVersion
  )
  install(FILES
    ${CMAKE_BINARY_DIR}/tinvestConfig.cmake
    ${CMAKE_BINARY_DIR}/tinvestConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tinvest
  )
endif()
