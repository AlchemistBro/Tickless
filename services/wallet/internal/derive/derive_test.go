package derive

import (
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/stretchr/testify/require"
)

// account returns a random account-level key (m/purpose'/coin'/0') and its xpub.
func account(t *testing.T, purpose, coin uint32, p *chaincfg.Params) (*hdkeychain.ExtendedKey, string) {
	t.Helper()
	seed, err := hdkeychain.GenerateSeed(hdkeychain.RecommendedSeedLen)
	require.NoError(t, err)
	k, err := hdkeychain.NewMaster(seed, p)
	require.NoError(t, err)
	for _, i := range []uint32{purpose, coin, 0} {
		k, err = k.Derive(hdkeychain.HardenedKeyStart + i)
		require.NoError(t, err)
	}
	pub, err := k.Neuter()
	require.NoError(t, err)
	return k, pub.String()
}

func TestBTCAddressesAreDeterministicDistinctAndRegtestBech32(t *testing.T) {
	_, xpub := account(t, 84, 1, &chaincfg.RegressionNetParams)
	a, err := NewBTC(xpub, &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	b, err := NewBTC(xpub, &chaincfg.RegressionNetParams)
	require.NoError(t, err)

	seen := map[string]uint32{}
	for i := uint32(0); i < 200; i++ {
		x, err := a.Address(i)
		require.NoError(t, err)
		y, err := b.Address(i)
		require.NoError(t, err)
		require.Equal(t, x, y, "same xpub and index must give the same address")
		require.True(t, strings.HasPrefix(x, "bcrt1q"), x)

		dec, err := btcutil.DecodeAddress(x, &chaincfg.RegressionNetParams)
		require.NoError(t, err)
		_, isP2WPKH := dec.(*btcutil.AddressWitnessPubKeyHash)
		require.True(t, isP2WPKH, "%s must be p2wpkh", x)

		prev, dup := seen[x]
		require.False(t, dup, "index %d repeats the address of index %d", i, prev)
		seen[x] = i
	}
}

// The address derived from the xpub must be the address of the corresponding
// child of the account key: proves the public-only path matches the key tree.
func TestXPubDerivationMatchesTheKeyTree(t *testing.T) {
	acct, xpub := account(t, 84, 1, &chaincfg.RegressionNetParams)
	d, err := NewBTC(xpub, &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	for _, i := range []uint32{0, 1, 7, 1000} {
		ext, err := acct.Derive(0)
		require.NoError(t, err)
		child, err := ext.Derive(i)
		require.NoError(t, err)
		pub, err := child.ECPubKey()
		require.NoError(t, err)
		want, err := btcutil.NewAddressWitnessPubKeyHash(btcutil.Hash160(pub.SerializeCompressed()), &chaincfg.RegressionNetParams)
		require.NoError(t, err)
		got, err := d.Address(i)
		require.NoError(t, err)
		require.Equal(t, want.EncodeAddress(), got, "index %d", i)
	}
}

var ethAddr = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

func TestETHAddressesAreChecksummedDeterministicAndDistinct(t *testing.T) {
	_, xpub := account(t, 44, 60, &chaincfg.MainNetParams)
	d, err := NewETH(xpub)
	require.NoError(t, err)
	seen := map[string]bool{}
	for i := uint32(0); i < 200; i++ {
		a, err := d.Address(i)
		require.NoError(t, err)
		require.Regexp(t, ethAddr, a)
		again, err := d.Address(i)
		require.NoError(t, err)
		require.Equal(t, a, again)

		raw, err := hex.DecodeString(strings.ToLower(a[2:]))
		require.NoError(t, err)
		require.Equal(t, a, ChecksumAddress(raw), "EIP-55 form must be canonical")
		require.False(t, seen[strings.ToLower(a)], "duplicate address at index %d", i)
		seen[strings.ToLower(a)] = true
	}
}

func TestChecksumAddressOnlyChangesLetterCase(t *testing.T) {
	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = byte(i*37 + 11)
	}
	got := ChecksumAddress(raw)
	require.Equal(t, "0x"+hex.EncodeToString(raw), strings.ToLower(got))
	require.NotEqual(t, strings.ToLower(got), got, "a checksum over these bytes must uppercase at least one letter")
}

func TestRejectsBadInput(t *testing.T) {
	acct, xpub := account(t, 84, 1, &chaincfg.RegressionNetParams)

	_, err := NewBTC(acct.String(), &chaincfg.RegressionNetParams)
	require.ErrorIs(t, err, ErrPrivateKey, "an extended private key must be refused")
	_, err = NewETH(acct.String())
	require.Error(t, err)

	_, err = NewBTC(xpub, &chaincfg.MainNetParams)
	require.ErrorContains(t, err, "not for network")

	_, err = NewBTC("garbage", &chaincfg.RegressionNetParams)
	require.Error(t, err)

	d, err := NewBTC(xpub, &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	_, err = d.Address(hdkeychain.HardenedKeyStart)
	require.ErrorContains(t, err, "hardened")
}
