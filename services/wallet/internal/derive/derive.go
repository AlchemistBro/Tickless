// Package derive turns an extended PUBLIC key into deposit addresses
// (BIP32 non-hardened derivation, ADR 0009). It never accepts a private key: a
// service that can only derive addresses cannot lose funds when it is breached.
package derive

import (
	"errors"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"golang.org/x/crypto/sha3"
)

// ErrPrivateKey is returned when a private extended key is supplied.
var ErrPrivateKey = errors.New("derive: a private extended key was supplied; only an xpub is accepted")

// maxIndex is the first hardened index; non-hardened children stop below it.
const maxIndex = hdkeychain.HardenedKeyStart

// Deriver derives the address at an index below one account xpub's external
// chain (account/0/index).
type Deriver interface {
	Address(index uint32) (string, error)
}

type external struct{ chain *hdkeychain.ExtendedKey }

func newExternal(xpub string, params *chaincfg.Params) (*external, error) {
	k, err := hdkeychain.NewKeyFromString(strings.TrimSpace(xpub))
	if err != nil {
		return nil, fmt.Errorf("derive: parse xpub: %w", err)
	}
	if k.IsPrivate() {
		return nil, ErrPrivateKey
	}
	if !k.IsForNet(params) {
		return nil, fmt.Errorf("derive: xpub is not for network %s", params.Name)
	}
	ext, err := k.Derive(0) // external (receive) chain
	if err != nil {
		return nil, fmt.Errorf("derive: external chain: %w", err)
	}
	return &external{chain: ext}, nil
}

func (e *external) child(index uint32) (*hdkeychain.ExtendedKey, error) {
	if index >= maxIndex {
		return nil, fmt.Errorf("derive: index %d is hardened", index)
	}
	return e.chain.Derive(index)
}

type btc struct {
	*external
	params *chaincfg.Params
}

// NewBTC returns a native-segwit (p2wpkh, bech32) deriver for params.
func NewBTC(xpub string, params *chaincfg.Params) (Deriver, error) {
	e, err := newExternal(xpub, params)
	if err != nil {
		return nil, err
	}
	return &btc{external: e, params: params}, nil
}

func (b *btc) Address(index uint32) (string, error) {
	k, err := b.child(index)
	if err != nil {
		return "", err
	}
	pub, err := k.ECPubKey()
	if err != nil {
		return "", err
	}
	addr, err := btcutil.NewAddressWitnessPubKeyHash(btcutil.Hash160(pub.SerializeCompressed()), b.params)
	if err != nil {
		return "", err
	}
	return addr.EncodeAddress(), nil
}

type eth struct{ *external }

// NewETH returns an Ethereum deriver (EIP-55 checksummed addresses). The
// xpub's network version bytes are irrelevant to Ethereum; mainnet "xpub"
// encoding is used for the key itself.
func NewETH(xpub string) (Deriver, error) {
	e, err := newExternal(xpub, &chaincfg.MainNetParams)
	if err != nil {
		return nil, err
	}
	return &eth{external: e}, nil
}

func (e *eth) Address(index uint32) (string, error) {
	k, err := e.child(index)
	if err != nil {
		return "", err
	}
	pub, err := k.ECPubKey()
	if err != nil {
		return "", err
	}
	// Address = last 20 bytes of keccak256(X || Y), without the 0x04 prefix.
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write(pub.SerializeUncompressed()[1:])
	return ChecksumAddress(h.Sum(nil)[12:]), nil
}

// ChecksumAddress renders 20 address bytes as an EIP-55 mixed-case address.
func ChecksumAddress(addr []byte) string {
	const hexdigits = "0123456789abcdef"
	lower := make([]byte, 40)
	for i, b := range addr {
		lower[2*i], lower[2*i+1] = hexdigits[b>>4], hexdigits[b&15]
	}
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write(lower)
	sum := h.Sum(nil)
	out := make([]byte, 40)
	for i, c := range lower {
		nib := sum[i/2] >> 4
		if i%2 == 1 {
			nib = sum[i/2] & 15
		}
		if c >= 'a' && nib >= 8 {
			c -= 'a' - 'A'
		}
		out[i] = c
	}
	return "0x" + string(out)
}
