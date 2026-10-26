// Package ethsign signs Ethereum payments from the hot wallet: native ETH
// transfers and ERC-20 transfer calls (Test USDT), as EIP-1559 transactions.
package ethsign

import (
	"errors"
	"math/big"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// NativeTransferGas is the gas of a plain ETH transfer to an EOA.
const NativeTransferGas = 21_000

// TransferSelector is the 4-byte selector of transfer(address,uint256).
var TransferSelector = crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]

// Params are the chain-dependent fields of the transaction.
type Params struct {
	ChainID   *big.Int
	Nonce     uint64
	GasTipCap *big.Int
	GasFeeCap *big.Int
	Gas       uint64 // 0 = NativeTransferGas for ETH; required for tokens
}

// Signed is a signed transaction.
type Signed struct {
	Raw    []byte
	TxID   string
	MaxFee *big.Int // gas * fee cap: an upper bound of the network fee
}

// TransferData encodes an ERC-20 transfer(to, amount) call.
func TransferData(to common.Address, amount *big.Int) []byte {
	data := append([]byte{}, TransferSelector...)
	data = append(data, common.LeftPadBytes(to.Bytes(), 32)...)
	return append(data, common.LeftPadBytes(amount.Bytes(), 32)...)
}

// Payment signs a payment of amount to dest. token is the ERC-20 contract for
// token payments, nil for native ETH.
func Payment(key *btcec.PrivateKey, token *common.Address, dest common.Address, amount *big.Int, p Params) (Signed, error) {
	if amount.Sign() <= 0 {
		return Signed{}, errors.New("ethsign: amount must be positive")
	}
	if p.ChainID == nil || p.GasTipCap == nil || p.GasFeeCap == nil {
		return Signed{}, errors.New("ethsign: chain id and fee caps are required")
	}
	inner := &types.DynamicFeeTx{ChainID: p.ChainID, Nonce: p.Nonce, GasTipCap: p.GasTipCap, GasFeeCap: p.GasFeeCap, Gas: p.Gas}
	if token == nil {
		inner.To, inner.Value = &dest, amount
		if inner.Gas == 0 {
			inner.Gas = NativeTransferGas
		}
	} else {
		if inner.Gas == 0 {
			return Signed{}, errors.New("ethsign: gas is required for token transfers")
		}
		inner.To, inner.Value, inner.Data = token, big.NewInt(0), TransferData(dest, amount)
	}
	// go-ethereum signs only with its own secp256k1 curve implementation.
	ek, err := crypto.ToECDSA(key.Serialize())
	if err != nil {
		return Signed{}, err
	}
	signed, err := types.SignTx(types.NewTx(inner), types.LatestSignerForChainID(p.ChainID), ek)
	if err != nil {
		return Signed{}, err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return Signed{}, err
	}
	maxFee := new(big.Int).Mul(new(big.Int).SetUint64(inner.Gas), p.GasFeeCap)
	return Signed{Raw: raw, TxID: signed.Hash().Hex(), MaxFee: maxFee}, nil
}
