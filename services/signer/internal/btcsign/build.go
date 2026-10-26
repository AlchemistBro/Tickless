// Package btcsign builds and signs native-segwit (p2wpkh) payments from the
// hot wallet. Building is a pure function of the spendable outputs, so it is
// verified by executing the signed scripts with btcd's script engine.
package btcsign

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

// DustLimit is the smallest output value relayed by Bitcoin Core for p2wpkh.
const DustLimit = 546

// ErrInsufficient means the confirmed spendable outputs do not cover amount + fee.
var ErrInsufficient = errors.New("btcsign: insufficient confirmed hot-wallet funds")

// UTXO is a spendable output of the hot wallet.
type UTXO struct {
	TxID     string
	Vout     uint32
	Value    int64 // satoshis
	PkScript []byte
}

// Outpoint returns "txid:vout".
func (u UTXO) Outpoint() string { return fmt.Sprintf("%s:%d", u.TxID, u.Vout) }

// Result is a signed transaction.
type Result struct {
	Tx    *wire.MsgTx
	Raw   []byte
	TxID  string
	Fee   int64
	Spent []string
}

// vsize estimate: version+locktime+counts+segwit marker (11) + per p2wpkh
// input 68 + per output 9 + script.
func vsize(nIn int, outScripts ...[]byte) int64 {
	v := int64(11 + 68*nIn)
	for _, s := range outScripts {
		v += int64(9 + len(s))
	}
	return v
}

// Build pays amount satoshis to dest from utxos (owned by key/from), at
// feeRate sat/vB, returning change above the dust limit to from. Outputs are
// chosen largest first (deterministic for equal inputs).
func Build(key *btcec.PrivateKey, from, dest btcutil.Address, utxos []UTXO, amount, feeRate int64) (Result, error) {
	if amount < DustLimit {
		return Result{}, fmt.Errorf("btcsign: amount %d is below the dust limit", amount)
	}
	destScript, err := txscript.PayToAddrScript(dest)
	if err != nil {
		return Result{}, err
	}
	changeScript, err := txscript.PayToAddrScript(from)
	if err != nil {
		return Result{}, err
	}
	sorted := append([]UTXO(nil), utxos...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Value != sorted[j].Value {
			return sorted[i].Value > sorted[j].Value
		}
		return sorted[i].Outpoint() < sorted[j].Outpoint()
	})
	var picked []UTXO
	var total int64
	fee := int64(0)
	for _, u := range sorted {
		picked = append(picked, u)
		total += u.Value
		fee = vsize(len(picked), destScript, changeScript) * feeRate
		if total >= amount+fee {
			break
		}
	}
	if total < amount+fee {
		return Result{}, fmt.Errorf("%w: have %d sat, need %d", ErrInsufficient, total, amount+fee)
	}
	tx := wire.NewMsgTx(2)
	fetcher := txscript.NewMultiPrevOutFetcher(nil)
	for _, u := range picked {
		h, err := chainhash.NewHashFromStr(u.TxID)
		if err != nil {
			return Result{}, err
		}
		op := wire.NewOutPoint(h, u.Vout)
		tx.AddTxIn(wire.NewTxIn(op, nil, nil))
		fetcher.AddPrevOut(*op, wire.NewTxOut(u.Value, u.PkScript))
	}
	tx.AddTxOut(wire.NewTxOut(amount, destScript))
	change := total - amount - fee
	if change >= DustLimit {
		tx.AddTxOut(wire.NewTxOut(change, changeScript))
	} else {
		fee += change // too small to be worth an output: it goes to the miner
	}
	hashes := txscript.NewTxSigHashes(tx, fetcher)
	for i, u := range picked {
		w, err := txscript.WitnessSignature(tx, hashes, i, u.Value, u.PkScript, txscript.SigHashAll, key, true)
		if err != nil {
			return Result{}, err
		}
		tx.TxIn[i].Witness = w
	}
	var buf bytes.Buffer
	if err := tx.Serialize(&buf); err != nil {
		return Result{}, err
	}
	spent := make([]string, len(picked))
	for i, u := range picked {
		spent[i] = u.Outpoint()
	}
	return Result{Tx: tx, Raw: buf.Bytes(), TxID: tx.TxHash().String(), Fee: fee, Spent: spent}, nil
}
