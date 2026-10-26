// Package keys holds the hot-wallet private keys of the signer. Keys are
// generated on first start inside the signer's own volume (file mode 0600)
// and never leave this service; other services only ever see addresses.
//
// Deliberate simplification (ADR 0011): local devnets only. A production
// signer would keep keys in an HSM/KMS and never on a plain file system.
package keys

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Wallet is the hot wallet of one deployment.
type Wallet struct {
	BTC    *btcec.PrivateKey
	ETH    *btcec.PrivateKey
	Params *chaincfg.Params
}

// LoadOrCreate reads btc.key and eth.key from dir, creating them if absent.
func LoadOrCreate(dir string, params *chaincfg.Params) (*Wallet, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	btcKey, err := loadOrCreate(filepath.Join(dir, "btc.key"))
	if err != nil {
		return nil, err
	}
	ethKey, err := loadOrCreate(filepath.Join(dir, "eth.key"))
	if err != nil {
		return nil, err
	}
	return &Wallet{BTC: btcKey, ETH: ethKey, Params: params}, nil
}

func loadOrCreate(path string) (*btcec.PrivateKey, error) {
	b, err := os.ReadFile(path) //nolint:gosec // path is inside the signer's own data directory
	if errors.Is(err, os.ErrNotExist) {
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, err
		}
		k, _ := btcec.PrivKeyFromBytes(raw[:])
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(hex.EncodeToString(k.Serialize())), 0o600); err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
		return k, nil
	}
	if err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("keys: %s is not a 32-byte hex key", path)
	}
	k, _ := btcec.PrivKeyFromBytes(raw)
	return k, nil
}

// BTCAddress is the hot wallet's native-segwit (p2wpkh) address.
func (w *Wallet) BTCAddress() (*btcutil.AddressWitnessPubKeyHash, error) {
	return btcutil.NewAddressWitnessPubKeyHash(btcutil.Hash160(w.BTC.PubKey().SerializeCompressed()), w.Params)
}

// ETHAddress is the hot wallet's Ethereum address.
func (w *Wallet) ETHAddress() common.Address {
	return crypto.PubkeyToAddress(w.ETH.ToECDSA().PublicKey)
}
