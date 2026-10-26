#!/usr/bin/env bash
# Rebuilds contracts/TestUSDT.bin (creation bytecode) with Foundry in Docker.
# The .bin is committed so `make up` never needs a compiler or the network.
set -euo pipefail
cd "$(dirname "$0")"
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/work" -w /work --entrypoint forge ghcr.io/foundry-rs/foundry:v1.2.3 \
  inspect TestUSDT.sol:TestUSDT bytecode > TestUSDT.bin.tmp
tr -d '\r\n' < TestUSDT.bin.tmp > TestUSDT.bin && rm TestUSDT.bin.tmp
echo "TestUSDT.bin: $(wc -c < TestUSDT.bin) hex chars"
