package rules

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/common"
)

// ErrAddress is returned for a destination that cannot receive funds on the chain.
var ErrAddress = errors.New("withdrawals: invalid destination address")

var ethHex = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// ValidateAddress checks the destination format and checksum for chain
// ("btc" on regtest/testnet, "eth"). Only addresses that decode and match
// the expected kind are accepted; an address on another network is refused.
func ValidateAddress(chain, address string) error {
	switch chain {
	case "btc":
		a, err := btcutil.DecodeAddress(address, &chaincfg.RegressionNetParams)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrAddress, err)
		}
		if !a.IsForNet(&chaincfg.RegressionNetParams) {
			return fmt.Errorf("%w: not a regtest address", ErrAddress)
		}
		switch a.(type) {
		case *btcutil.AddressWitnessPubKeyHash, *btcutil.AddressWitnessScriptHash:
			return nil
		default:
			return fmt.Errorf("%w: only native segwit (bcrt1q...) addresses are supported", ErrAddress)
		}
	case "eth":
		if !ethHex.MatchString(address) {
			return fmt.Errorf("%w: expected 0x followed by 40 hex digits", ErrAddress)
		}
		// An all-lower or all-upper address carries no checksum and is accepted;
		// a mixed-case one must match EIP-55 exactly, which catches typos.
		if strings.ToLower(address) != address && strings.ToUpper(address[2:]) != address[2:] {
			if common.HexToAddress(address).Hex() != address {
				return fmt.Errorf("%w: EIP-55 checksum mismatch", ErrAddress)
			}
		}
		if common.HexToAddress(address) == (common.Address{}) {
			return fmt.Errorf("%w: the zero address", ErrAddress)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown chain %q", ErrAddress, chain)
	}
}
