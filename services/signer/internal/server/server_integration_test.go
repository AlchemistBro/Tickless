//go:build integration

package server_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	signerv1 "github.com/tickless/pkg/gen/tickless/signer/v1"
	"github.com/tickless/pkg/testkit"
	"github.com/tickless/signer/internal/journal"
	"github.com/tickless/signer/internal/keys"
	"github.com/tickless/signer/internal/server"
)

func newServer(t *testing.T) *server.Server {
	t.Helper()
	dir := t.TempDir()
	w, err := keys.LoadOrCreate(dir, &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	j, err := journal.Open(dir + "/journal")
	require.NoError(t, err)
	return &server.Server{Wallet: w, Journal: j, BTCFeeRate: 2, ETHTip: big.NewInt(1_000_000_000)}
}

func req(id int64, chain, asset, units, dest string) *signerv1.SignWithdrawalRequest {
	return &signerv1.SignWithdrawalRequest{WithdrawalId: id, Chain: chain, Asset: asset, Amount: &commonv1.Amount{Units: units}, Destination: dest}
}

func TestBTCWithdrawalsArePaidOnceFromTheHotWallet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	url, user, pass := testkit.Bitcoind(t)
	s := newServer(t)
	s.BTC = server.NewBitcoindRPC(url, user, pass)
	node := s.BTC
	faucet := server.NewBitcoindRPC(url+"/wallet/faucet", user, pass)
	payee := server.NewBitcoindRPC(url+"/wallet/payee", user, pass)
	require.NoError(t, node.Call(ctx, "createwallet", nil, "faucet"))
	require.NoError(t, node.Call(ctx, "createwallet", nil, "payee"))
	var mineTo, dest1, dest2 string
	require.NoError(t, faucet.Call(ctx, "getnewaddress", &mineTo, "", "bech32"))
	require.NoError(t, payee.Call(ctx, "getnewaddress", &dest1, "", "bech32"))
	require.NoError(t, payee.Call(ctx, "getnewaddress", &dest2, "", "bech32"))
	require.NoError(t, node.Call(ctx, "generatetoaddress", nil, 101, mineTo))

	hot, err := s.HotWallet(ctx, &signerv1.HotWalletRequest{Chain: "btc"})
	require.NoError(t, err)
	// Two separate outputs fund the hot wallet.
	require.NoError(t, faucet.Call(ctx, "sendtoaddress", nil, hot.Address, "0.5"))
	require.NoError(t, faucet.Call(ctx, "sendtoaddress", nil, hot.Address, "0.4"))
	require.NoError(t, node.Call(ctx, "generatetoaddress", nil, 1, mineTo))

	r1, err := s.SignWithdrawal(ctx, req(1, "btc", "BTC", "10000000", dest1)) // 0.1 BTC
	require.NoError(t, err)
	again, err := s.SignWithdrawal(ctx, req(1, "btc", "BTC", "10000000", dest1))
	require.NoError(t, err)
	require.Equal(t, r1.Txid, again.Txid, "a retry returns the same transaction")
	require.Equal(t, r1.RawTx, again.RawTx)
	_, err = s.SignWithdrawal(ctx, req(1, "btc", "BTC", "10000001", dest1))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "same id, other terms")

	var txid string
	require.NoError(t, node.Call(ctx, "sendrawtransaction", &txid, hex.EncodeToString(r1.RawTx)))
	require.Equal(t, r1.Txid, txid)

	// Before the next block, a second withdrawal must not reuse the spent output.
	r2, err := s.SignWithdrawal(ctx, req(2, "btc", "BTC", "20000000", dest2))
	require.NoError(t, err)
	require.NoError(t, node.Call(ctx, "sendrawtransaction", nil, hex.EncodeToString(r2.RawTx)), "no mempool conflict with withdrawal 1")

	// A third one has nothing confirmed left to spend until the change confirms.
	_, err = s.SignWithdrawal(ctx, req(3, "btc", "BTC", "10000000", dest1))
	require.Equal(t, codes.Unavailable, status.Code(err))

	require.NoError(t, node.Call(ctx, "generatetoaddress", nil, 1, mineTo))
	var got1, got2 json.Number
	require.NoError(t, payee.Call(ctx, "getreceivedbyaddress", &got1, dest1))
	require.NoError(t, payee.Call(ctx, "getreceivedbyaddress", &got2, dest2))
	require.Equal(t, "0.10000000", got1.String())
	require.Equal(t, "0.20000000", got2.String())

	// After the block, the change is confirmed and pays withdrawal 3.
	r3, err := s.SignWithdrawal(ctx, req(3, "btc", "BTC", "10000000", dest1))
	require.NoError(t, err)
	require.NoError(t, node.Call(ctx, "sendrawtransaction", nil, hex.EncodeToString(r3.RawTx)))
}

func TestETHAndUSDTWithdrawalsOnAnvil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c, err := rpc.DialContext(ctx, testkit.Anvil(t))
	require.NoError(t, err)
	defer c.Close()
	s := newServer(t)
	s.ETH = c
	var accounts []common.Address
	require.NoError(t, c.CallContext(ctx, &accounts, "eth_accounts"))
	dev := accounts[0]

	send := func(tx map[string]any) map[string]any {
		var h common.Hash
		require.NoError(t, c.CallContext(ctx, &h, "eth_sendTransaction", tx))
		var rcpt map[string]any
		for i := 0; i < 100 && rcpt == nil; i++ {
			require.NoError(t, c.CallContext(ctx, &rcpt, "eth_getTransactionReceipt", h))
			if rcpt == nil {
				time.Sleep(50 * time.Millisecond)
			}
		}
		require.NotNil(t, rcpt)
		require.Equal(t, "0x1", rcpt["status"])
		return rcpt
	}
	bin, err := os.ReadFile("../../../../contracts/TestUSDT.bin")
	require.NoError(t, err)
	token := common.HexToAddress(send(map[string]any{"from": dev, "data": strings.TrimSpace(string(bin)), "gas": "0x200000"})["contractAddress"].(string))
	s.Token = token

	hot, err := s.HotWallet(ctx, &signerv1.HotWalletRequest{Chain: "eth"})
	require.NoError(t, err)
	send(map[string]any{"from": dev, "to": hot.Address, "value": hexutil.EncodeBig(big.NewInt(1_000_000_000_000_000_000))})
	mint := append(append(append([]byte{}, crypto.Keccak256([]byte("mint(address,uint256)"))[:4]...),
		common.LeftPadBytes(common.HexToAddress(hot.Address).Bytes(), 32)...), common.LeftPadBytes(big.NewInt(500_000_000).Bytes(), 32)...)
	send(map[string]any{"from": dev, "to": token, "data": hexutil.Bytes(mint)})

	dest := common.HexToAddress("0x2222222222222222222222222222222222222222")
	broadcast := func(r *signerv1.SignWithdrawalResponse) {
		var h common.Hash
		require.NoError(t, c.CallContext(ctx, &h, "eth_sendRawTransaction", hexutil.Bytes(r.RawTx)))
		require.Equal(t, r.Txid, h.Hex())
	}
	eth1, err := s.SignWithdrawal(ctx, req(10, "eth", "ETH", "250000000000000000", dest.Hex()))
	require.NoError(t, err)
	usdt, err := s.SignWithdrawal(ctx, req(11, "eth", "USDT", "12500000", dest.Hex()))
	require.NoError(t, err)
	again, err := s.SignWithdrawal(ctx, req(10, "eth", "ETH", "250000000000000000", dest.Hex()))
	require.NoError(t, err)
	require.Equal(t, eth1.Txid, again.Txid, "idempotent by withdrawal id")
	broadcast(eth1)
	broadcast(usdt)
	time.Sleep(1500 * time.Millisecond)

	var bal hexutil.Big
	require.NoError(t, c.CallContext(ctx, &bal, "eth_getBalance", dest, "latest"))
	require.Equal(t, "250000000000000000", bal.ToInt().String())
	call := append(append([]byte{}, crypto.Keccak256([]byte("balanceOf(address)"))[:4]...), common.LeftPadBytes(dest.Bytes(), 32)...)
	var out hexutil.Bytes
	require.NoError(t, c.CallContext(ctx, &out, "eth_call", map[string]any{"to": token, "data": hexutil.Bytes(call)}, "latest"))
	require.Equal(t, "12500000", new(big.Int).SetBytes(out).String())
}
