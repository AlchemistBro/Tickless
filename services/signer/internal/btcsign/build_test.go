package btcsign

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
)

func wallet(t *testing.T) (*btcec.PrivateKey, btcutil.Address, []byte) {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	k, _ := btcec.PrivKeyFromBytes(raw[:])
	a, err := btcutil.NewAddressWitnessPubKeyHash(btcutil.Hash160(k.PubKey().SerializeCompressed()), &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	s, err := txscript.PayToAddrScript(a)
	require.NoError(t, err)
	return k, a, s
}

func utxos(script []byte, values ...int64) []UTXO {
	out := make([]UTXO, len(values))
	for i, v := range values {
		out[i] = UTXO{TxID: fmt.Sprintf("%064x", i+1), Vout: uint32(i), Value: v, PkScript: script} //nolint:gosec // small test indexes
	}
	return out
}

// verify executes every input's scripts, exactly as a node would.
func verify(t *testing.T, tx *wire.MsgTx, ins []UTXO, spent []string) {
	t.Helper()
	fetcher := txscript.NewMultiPrevOutFetcher(nil)
	byOp := map[string]UTXO{}
	for _, u := range ins {
		byOp[u.Outpoint()] = u
	}
	for _, in := range tx.TxIn {
		u := byOp[fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash, in.PreviousOutPoint.Index)]
		fetcher.AddPrevOut(in.PreviousOutPoint, wire.NewTxOut(u.Value, u.PkScript))
	}
	hashes := txscript.NewTxSigHashes(tx, fetcher)
	require.Len(t, tx.TxIn, len(spent))
	for i, in := range tx.TxIn {
		prev := fetcher.FetchPrevOutput(in.PreviousOutPoint)
		e, err := txscript.NewEngine(prev.PkScript, tx, i, txscript.StandardVerifyFlags, nil, hashes, prev.Value, fetcher)
		require.NoError(t, err)
		require.NoError(t, e.Execute(), "input %d must validate", i)
	}
}

func TestBuildPaysExactlyAndReturnsChange(t *testing.T) {
	key, from, script := wallet(t)
	_, dest, destScript := wallet(t)
	ins := utxos(script, 30_000, 120_000, 50_000)
	r, err := Build(key, from, dest, ins, 100_000, 2)
	require.NoError(t, err)
	verify(t, r.Tx, ins, r.Spent)

	require.Len(t, r.Tx.TxIn, 1, "the largest output alone covers 100k + fee")
	require.Equal(t, []string{ins[1].Outpoint()}, r.Spent)
	require.Len(t, r.Tx.TxOut, 2)
	require.Equal(t, int64(100_000), r.Tx.TxOut[0].Value)
	require.Equal(t, destScript, r.Tx.TxOut[0].PkScript)
	require.Equal(t, script, r.Tx.TxOut[1].PkScript, "change returns to the hot wallet")
	require.Equal(t, int64(120_000), r.Tx.TxOut[0].Value+r.Tx.TxOut[1].Value+r.Fee, "inputs = outputs + fee")

	vs := blockchain.GetTransactionWeight(btcutil.NewTx(r.Tx)) / 4
	require.GreaterOrEqual(t, r.Fee, vs*2, "the paid fee is at least 2 sat/vB of the real size")
	require.Equal(t, r.Tx.TxHash().String(), r.TxID)
}

func TestBuildCombinesInputsAndDropsDustChange(t *testing.T) {
	key, from, script := wallet(t)
	_, dest, _ := wallet(t)
	ins := utxos(script, 60_000, 50_000)
	// 109,600 + fee leaves less than the dust limit as change.
	r, err := Build(key, from, dest, ins, 109_600, 1)
	require.NoError(t, err)
	verify(t, r.Tx, ins, r.Spent)
	require.Len(t, r.Tx.TxIn, 2)
	require.Len(t, r.Tx.TxOut, 1, "change below the dust limit is not an output")
	require.Equal(t, int64(110_000), r.Tx.TxOut[0].Value+r.Fee)
}

func TestBuildRefusesWhatItCannotPay(t *testing.T) {
	key, from, script := wallet(t)
	_, dest, _ := wallet(t)
	_, err := Build(key, from, dest, utxos(script, 10_000), 10_000, 1)
	require.ErrorIs(t, err, ErrInsufficient, "the fee must fit too")
	_, err = Build(key, from, dest, nil, 1_000, 1)
	require.ErrorIs(t, err, ErrInsufficient)
	_, err = Build(key, from, dest, utxos(script, 10_000), 100, 1)
	require.Error(t, err, "dust payments are refused")
}

func TestOtherKeyCannotSpend(t *testing.T) {
	_, from, script := wallet(t)
	other, _, _ := wallet(t)
	_, dest, _ := wallet(t)
	ins := utxos(script, 100_000)
	r, err := Build(other, from, dest, ins, 50_000, 1)
	require.NoError(t, err, "building succeeds, but the signature is wrong")
	fetcher := txscript.NewMultiPrevOutFetcher(nil)
	fetcher.AddPrevOut(r.Tx.TxIn[0].PreviousOutPoint, wire.NewTxOut(100_000, script))
	e, err := txscript.NewEngine(script, r.Tx, 0, txscript.StandardVerifyFlags, nil, txscript.NewTxSigHashes(r.Tx, fetcher), 100_000, fetcher)
	require.NoError(t, err)
	require.Error(t, e.Execute(), "a foreign key does not satisfy the hot wallet's script")
}
