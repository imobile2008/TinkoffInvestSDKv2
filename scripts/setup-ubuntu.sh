#!/usr/bin/env bash
# Bootstrap a clean Ubuntu (22.04/24.04) machine: install toolchain and
# gRPC/protobuf, then configure, build and run the tests.
# Usage: ./scripts/setup-ubuntu.sh
set -euo pipefail
cd "$(dirname "$0")/.."

SUDO=""
if [ "$(id -u)" -ne 0 ]; then SUDO="sudo"; fi

$SUDO apt-get update
$SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake ninja-build pkg-config git ca-certificates curl \
  protobuf-compiler protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev

# invest-public-api.tbank.ru is signed by the Russian Trusted CA (Ministry of
# Digital Development). Install it into the system trust store — the SDK reads
# /etc/ssl/certs/ca-certificates.crt by default.
if [ ! -f /usr/local/share/ca-certificates/russian-trusted/russian_trusted_root_ca.crt ]; then
  $SUDO mkdir -p /usr/local/share/ca-certificates/russian-trusted
  $SUDO curl -sSL -o /usr/local/share/ca-certificates/russian-trusted/russian_trusted_root_ca.crt \
    https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt
  $SUDO curl -sSL -o /usr/local/share/ca-certificates/russian-trusted/russian_trusted_sub_ca.crt \
    https://gu-st.ru/content/lending/russian_trusted_sub_ca_pem.crt
  $SUDO update-ca-certificates
fi

# The SDK uses the modern gRPC callback API (stub->async(), ClientBidiReactor
# in the grpc:: namespace) which needs gRPC >= 1.46. Ubuntu 24.04 ships 1.51;
# Ubuntu 22.04 ships 1.30 which is too old — build gRPC from source there.
GRPC_VER=$(pkg-config --modversion grpc++ 2>/dev/null || echo "0")
echo "System gRPC version: ${GRPC_VER}"
if dpkg --compare-versions "${GRPC_VER}" lt "1.46"; then
  echo "gRPC ${GRPC_VER} is too old; building gRPC v1.54.3 from source (~15 min)..."
  git clone --recurse-submodules -b v1.54.3 --depth 1 --shallow-submodules \
    https://github.com/grpc/grpc /tmp/grpc-src
  cmake -S /tmp/grpc-src -B /tmp/grpc-build -G Ninja \
    -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local
  cmake --build /tmp/grpc-build -j"$(nproc)"
  $SUDO cmake --install /tmp/grpc-build
  $SUDO ldconfig
fi

cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure

echo
echo "Done. Examples: build/examples/{accounts,quotes_stream,sandbox_trade}"
echo "Run with: TINVEST_TOKEN=t.xxx ./build/examples/accounts"
