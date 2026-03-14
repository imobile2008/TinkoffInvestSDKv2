# TinkoffInvestSDKv2 Task TODO: Integrate vcpkg for libraries

## Vcpkg Integration Plan (Approved):
1. [x] Create vcpkg.json manifest for gRPC, protobuf.
2. [x] Update cmake/common.cmake: Replace FetchContent with find_package for gRPC/protobuf.
3. [x] Update root CMakeLists.txt: Support vcpkg toolchain.
4. [ ] Update sample CMakeLists.txt files if needed.
5. [ ] Test CMake configure/build.
6. [ ] Update TODO.md complete.

## Previous C++20 Upgrade and Build Fix TODO:
## Steps:
- [x] Step 1: Update CMakeLists.txt, cmake/common.cmake, samples/*/*.cmake to C++20
- [x] Step 2: CMake C++20 config successful (GCC 13.3)
- [x] Step 3 (partial): instrumentsservice.cpp, marketdatastreamservice.cpp refactored to unique_ptr
- [ ] Step 4: Update customservice.cpp and rpchandler.cpp for std::jthread
- [ ] Step 5: Build and test samples/unary, stream_sync, stream_async, tests/
- [ ] Step 6: Full project build and completion

## Build Fix Progress:
- [x] Diagnosed gRPC FetchContent corrupted clone in build/_deps
- [x] Clean build/ directory
- [x] Update investAPI submodule
- [x] CMake configure re-run successfully (gRPC downloading, no errors)
- [x] Enhanced common.cmake (shallow clone, latest gRPC v1.66.1)
- [ ] Run 'cmake --build build' after configure finishes
- [ ] Test samples (cd samples/unary && cmake --build . etc.)
