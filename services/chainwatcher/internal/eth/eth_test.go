package eth

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

var (
	token = common.HexToAddress("0x5FbDB2315678afecb367f032d93F642f64180aa3")
	other = common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	alice = common.HexToAddress("0x70997970C51812dc3A010C7d01b50e0d17dc79C8")
)

func h(n byte) common.Hash { return common.BytesToHash([]byte{n}) }

func val(v int64) *hexutil.Big { return (*hexutil.Big)(big.NewInt(v)) }

func word(v *big.Int) hexutil.Bytes { return common.LeftPadBytes(v.Bytes(), 32) }

func transferLog(contract, to common.Address, v *big.Int, idx uint64) rpcLog {
	return rpcLog{Address: contract, Topics: []common.Hash{TransferTopic, common.BytesToHash(other.Bytes()), common.BytesToHash(to.Bytes())}, Data: word(v), LogIndex: hexutil.Uint64(idx)}
}

func TestToBlockKeepsOnlySuccessfulValueAndTokenTransfers(t *testing.T) {
	c := Chain{Token: token, TokenAsset: "USDT"}
	b := &rpcBlock{Number: 7, Hash: h(7), ParentHash: h(6), Transactions: []rpcTx{
		{Hash: h(1), To: &alice, Value: val(1_000_000_000_000_000_000)}, // 1 ETH, ok
		{Hash: h(2), To: &alice, Value: val(5)},                         // reverted
		{Hash: h(3), To: nil, Value: val(9)},                            // contract creation
		{Hash: h(4), To: &token, Value: val(0)},                         // token call, no value
	}}
	removed := transferLog(token, alice, big.NewInt(3), 2)
	removed.Removed = true
	receipts := []rpcReceipt{
		{TxHash: h(1), Status: 1},
		{TxHash: h(2), Status: 0, Logs: []rpcLog{transferLog(token, alice, big.NewInt(99), 0)}},
		{TxHash: h(3), Status: 1},
		{TxHash: h(4), Status: 1, Logs: []rpcLog{
			transferLog(token, alice, big.NewInt(2_500_000), 0), // 2.5 USDT, ok
			transferLog(other, alice, big.NewInt(7), 1),         // another contract's event
			removed,
		}},
	}
	got, err := c.toBlock(b, receipts)
	require.NoError(t, err)
	require.EqualValues(t, 7, got.Height)
	require.Equal(t, h(7).Hex(), got.Hash)
	require.Equal(t, h(6).Hex(), got.Parent)
	require.Len(t, got.Transfers, 2)

	eth := got.Transfers[0]
	require.Equal(t, h(1).Hex(), eth.TxID)
	require.Equal(t, NativeIndex, eth.Index)
	require.Equal(t, "ETH", eth.Asset)
	require.Equal(t, alice.Hex(), eth.Address)
	require.Equal(t, "1000000000000000000", eth.Amount.String())

	usdt := got.Transfers[1]
	require.Equal(t, h(4).Hex(), usdt.TxID)
	require.EqualValues(t, 0, usdt.Index)
	require.Equal(t, "USDT", usdt.Asset)
	require.Equal(t, alice.Hex(), usdt.Address)
	require.Equal(t, "2500000", usdt.Amount.String())
}

func TestToBlockRefusesTokenAmountsBeyondTheMoneyRange(t *testing.T) {
	huge := new(big.Int).Lsh(big.NewInt(1), 200)
	c := Chain{Token: token, TokenAsset: "USDT"}
	b := &rpcBlock{Number: 1, Hash: h(1), Transactions: []rpcTx{{Hash: h(9), To: &token, Value: val(0)}}}
	_, err := c.toBlock(b, []rpcReceipt{{TxHash: h(9), Status: 1, Logs: []rpcLog{transferLog(token, alice, huge, 0)}}})
	require.Error(t, err)
}

func TestWithoutTokenOnlyNativeTransfersCount(t *testing.T) {
	c := Chain{}
	b := &rpcBlock{Number: 1, Hash: h(1), Transactions: []rpcTx{{Hash: h(9), To: &token, Value: val(0)}}}
	got, err := c.toBlock(b, []rpcReceipt{{TxHash: h(9), Status: 1, Logs: []rpcLog{transferLog(token, alice, big.NewInt(1), 0)}}})
	require.NoError(t, err)
	require.Empty(t, got.Transfers)
}

func TestParseToken(t *testing.T) {
	a, err := ParseToken(" 0x5FbDB2315678afecb367f032d93F642f64180aa3 ")
	require.NoError(t, err)
	require.Equal(t, token, a)
	z, err := ParseToken("")
	require.NoError(t, err)
	require.Equal(t, common.Address{}, z)
	_, err = ParseToken("0x123")
	require.Error(t, err)
}
