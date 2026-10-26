// Command devxpub prints a fresh pair of account-level extended PUBLIC keys
// for the local devnets (BTC regtest m/84'/1'/0', ETH m/44'/60'/0').
//
// The seed is generated in memory and discarded when the program exits; it is
// never printed or written anywhere. Funds sent to the derived deposit
// addresses are therefore unspendable, which is acceptable on regtest/Anvil
// only and is stated in ADR 0009. Run it once and paste the output into
// deploy/compose (WALLET_BTC_XPUB, WALLET_ETH_XPUB).
package main

import (
	"fmt"
	"os"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
)

func accountXPub(seed []byte, purpose, coin uint32, params *chaincfg.Params) (string, error) {
	k, err := hdkeychain.NewMaster(seed, params)
	if err != nil {
		return "", err
	}
	for _, i := range []uint32{purpose, coin, 0} {
		if k, err = k.Derive(hdkeychain.HardenedKeyStart + i); err != nil {
			return "", err
		}
	}
	pub, err := k.Neuter()
	if err != nil {
		return "", err
	}
	return pub.String(), nil
}

func main() {
	seed, err := hdkeychain.GenerateSeed(hdkeychain.RecommendedSeedLen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	btc, err := accountXPub(seed, 84, 1, &chaincfg.RegressionNetParams)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	eth, err := accountXPub(seed, 44, 60, &chaincfg.MainNetParams)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	clear(seed)
	fmt.Printf("WALLET_BTC_XPUB=%s\nWALLET_ETH_XPUB=%s\n", btc, eth)
}
