#!/usr/bin/env bash
# Refreshes contracts/ from the upstream T-Invest API repository
# (mirror of https://opensource.tbank.ru/invest/invest-contracts).
set -euo pipefail
cd "$(dirname "$0")/.."

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git clone --depth 1 https://github.com/RussianInvestments/investAPI "$tmp"
rm -rf contracts
mkdir -p contracts
cp -r "$tmp"/src/docs/contracts/. contracts/
echo "contracts/ updated:"
ls contracts
