package ethsign

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

func key(t *testing.T) (*btcec.PrivateKey, common.Address) {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	k, _ := btcec.PrivKeyFromBytes(raw[:])
	return k, crypto.PubkeyToAddress(k.ToECDSA().PublicKey)
}

var params = Params{ChainID: big.NewInt(31337), Nonce: 7, GasTipCap: big.NewInt(1_000_000_000), GasFeeCap: big.NewInt(3_000_000_000)}

func decode(t *testing.T, raw []byte) *types.Transaction {
	t.Helper()
	var tx types.Transaction
	require.NoError(t, tx.UnmarshalBinary(raw))
	return &tx
}

func TestNativePaymentIsSignedByTheHotWallet(t *testing.T) {
	k, hot := key(t)
	_, dest := key(t)
	s, err := Payment(k, nil, dest, big.NewInt(250_000_000_000_000_000), params)
	require.NoError(t, err)
	tx := decode(t, s.Raw)
	from, err := types.Sender(types.LatestSignerForChainID(params.ChainID), tx)
	require.NoError(t, err)
	require.Equal(t, hot, from, "the signature recovers to the hot wallet")
	require.Equal(t, dest, *tx.To())
	require.Equal(t, "250000000000000000", tx.Value().String())
	require.EqualValues(t, NativeTransferGas, tx.Gas())
	require.EqualValues(t, 7, tx.Nonce())
	require.Equal(t, uint8(types.DynamicFeeTxType), tx.Type())
	require.Equal(t, tx.Hash().Hex(), s.TxID)
	require.Equal(t, "63000000000000", s.MaxFee.String(), "21000 gas * 3 gwei")
}

func TestTokenPaymentCallsTransfer(t *testing.T) {
	k, _ := key(t)
	_, dest := key(t)
	token := common.HexToAddress("0x5FbDB2315678afecb367f032d93F642f64180aa3")
	p := params
	p.Gas = 60_000
	s, err := Payment(k, &token, dest, big.NewInt(12_500_000), p)
	require.NoError(t, err)
	tx := decode(t, s.Raw)
	require.Equal(t, token, *tx.To(), "a token payment is a call to the token contract")
	require.Zero(t, tx.Value().Sign(), "no ETH is sent with it")
	data := tx.Data()
	require.Len(t, data, 4+32+32)
	require.Equal(t, TransferSelector, data[:4])
	require.Equal(t, "a9059cbb", common.Bytes2Hex(data[:4]), "well-known selector of transfer(address,uint256)")
	require.Equal(t, dest, common.BytesToAddress(data[4:36]))
	require.Equal(t, "12500000", new(big.Int).SetBytes(data[36:]).String())
}

func TestPaymentRefusesBadInput(t *testing.T) {
	k, _ := key(t)
	_, dest := key(t)
	token := common.HexToAddress("0x5FbDB2315678afecb367f032d93F642f64180aa3")
	_, err := Payment(k, nil, dest, big.NewInt(0), params)
	require.Error(t, err)
	_, err = Payment(k, &token, dest, big.NewInt(1), params)
	require.Error(t, err, "token transfers need an explicit gas limit")
	_, err = Payment(k, nil, dest, big.NewInt(1), Params{})
	require.Error(t, err)
}
