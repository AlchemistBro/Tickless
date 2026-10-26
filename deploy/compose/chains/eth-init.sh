#!/bin/sh
# Deploys the Test USDT contract (contracts/TestUSDT.bin) on the Anvil devnet
# from Anvil's first dev account, which Anvil keeps unlocked: no private key is
# used or stored. The first contract of that account on a fresh chain always
# lands at the address below (CREATE address of nonce 0). Idempotent.
set -eu
RPC=http://anvil:8545
FROM=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # Anvil dev account 0 (public address)
TOKEN=0x5FbDB2315678afecb367f032d93F642f64180aa3  # CREATE(FROM, nonce 0)
until cast block-number --rpc-url "$RPC" >/dev/null 2>&1; do sleep 1; done
if [ "$(cast code "$TOKEN" --rpc-url "$RPC")" = "0x" ]; then
  cast send --rpc-url "$RPC" --unlocked --from "$FROM" --create "$(cat /contracts/TestUSDT.bin)" >/dev/null
fi
if [ "$(cast code "$TOKEN" --rpc-url "$RPC")" = "0x" ]; then
  echo "Test USDT is not at $TOKEN (was the dev account used before?). Reset the devnet: make down" >&2
  exit 1
fi
echo "Test USDT at $TOKEN, symbol $(cast call "$TOKEN" 'symbol()(string)' --rpc-url "$RPC"), decimals $(cast call "$TOKEN" 'decimals()(uint8)' --rpc-url "$RPC")"
