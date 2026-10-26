#!/bin/sh
# Prepares the regtest node: a "faucet" wallet with mature coins (101 blocks)
# for the e2e suite and the demo. Idempotent. Local regtest only.
set -eu
CLI="bitcoin-cli -regtest -rpcconnect=bitcoind -rpcuser=${BTC_RPC_USER} -rpcpassword=${BTC_RPC_PASSWORD}"
until $CLI getblockchaininfo >/dev/null 2>&1; do sleep 1; done
$CLI loadwallet faucet >/dev/null 2>&1 || $CLI createwallet faucet >/dev/null 2>&1 || true
ADDR=$($CLI -rpcwallet=faucet getnewaddress faucet bech32)
H=$($CLI getblockcount)
if [ "$H" -lt 101 ]; then
  $CLI generatetoaddress $((101 - H)) "$ADDR" >/dev/null
fi
echo "bitcoind ready: height $($CLI getblockcount), faucet balance $($CLI -rpcwallet=faucet getbalance) BTC"
