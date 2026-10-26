//go:build integration

package eth_test

import (
	"context"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/tickless/chainwatcher/internal/eth"
	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/chainwatcher/internal/store"
	"github.com/tickless/chainwatcher/migrations"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

type devnet struct {
	t        *testing.T
	ctx      context.Context
	c        *rpc.Client
	accounts []common.Address
}

func (d *devnet) call(out any, method string, args ...any) {
	d.t.Helper()
	require.NoError(d.t, d.c.CallContext(d.ctx, out, method, args...), method)
}

// send submits a transaction from an unlocked dev account and returns its receipt.
func (d *devnet) send(tx map[string]any) map[string]any {
	d.t.Helper()
	var hash common.Hash
	d.call(&hash, "eth_sendTransaction", tx)
	var receipt map[string]any
	for i := 0; i < 100 && receipt == nil; i++ { // automine is quick but asynchronous
		d.call(&receipt, "eth_getTransactionReceipt", hash)
		if receipt == nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
	require.NotNil(d.t, receipt, "no receipt for %s", hash)
	return receipt
}

func (d *devnet) mine(n int) { d.call(nil, "anvil_mine", hexutil.EncodeUint64(uint64(n))) } //nolint:gosec // small test counts

func calldata(sig string, args ...[]byte) hexutil.Bytes {
	out := crypto.Keccak256([]byte(sig))[:4]
	for _, a := range args {
		out = append(out, common.LeftPadBytes(a, 32)...)
	}
	return out
}

func TestAnvilNativeAndTokenDepositsAreDetectedAndConfirmed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c, err := rpc.DialContext(ctx, testkit.Anvil(t))
	require.NoError(t, err)
	defer c.Close()
	d := &devnet{t: t, ctx: ctx, c: c}
	d.call(&d.accounts, "eth_accounts")
	funder, poor := d.accounts[0], d.accounts[9]

	// Deploy the committed Test USDT bytecode (contracts/TestUSDT.bin).
	bin, err := os.ReadFile("../../../../contracts/TestUSDT.bin")
	require.NoError(t, err)
	r := d.send(map[string]any{"from": funder, "data": strings.TrimSpace(string(bin)), "gas": "0x200000"})
	require.Equal(t, "0x1", r["status"])
	token := common.HexToAddress(r["contractAddress"].(string))

	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 10)
	require.NoError(t, err)
	defer pool.Close()
	st := store.New(pool)
	deposit := common.HexToAddress("0x1111111111111111111111111111111111111111")
	require.NoError(t, st.RegisterAddress(ctx, "eth", deposit.Hex(), 5, 0))

	s := &scan.Scanner{Chain: eth.Chain{RPC: c, Token: token, TokenAsset: "USDT"}, Store: st.Chain("eth"), Name: "eth", Start: 1, Required: 12, MaxBatch: 500}

	// 0.75 ETH, 12.5 USDT, and a token transfer that reverts (no balance).
	ethTx := d.send(map[string]any{"from": funder, "to": deposit, "value": hexutil.EncodeBig(big.NewInt(750_000_000_000_000_000))})
	mintTx := d.send(map[string]any{"from": funder, "to": token, "data": calldata("mint(address,uint256)", deposit.Bytes(), big.NewInt(12_500_000).Bytes())})
	revert := d.send(map[string]any{"from": poor, "to": token, "gas": "0x30000", "data": calldata("transfer(address,uint256)", deposit.Bytes(), big.NewInt(1).Bytes())})
	require.Equal(t, "0x0", revert["status"], "the transfer without balance must revert")

	for {
		n, err := s.Step(ctx)
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	ds, err := st.ListDeposits(ctx, 5, 0, 10)
	require.NoError(t, err)
	require.Len(t, ds, 2, "the reverted transfer is not a deposit")
	byAsset := map[string]store.Deposit{}
	for _, dp := range ds {
		byAsset[dp.Asset] = dp
		require.Equal(t, store.StatusPending, dp.Status)
	}
	require.Equal(t, "750000000000000000", byAsset["ETH"].Amount.String())
	require.Equal(t, eth.NativeIndex, byAsset["ETH"].Index)
	require.Equal(t, ethTx["transactionHash"], byAsset["ETH"].TxID)
	require.Equal(t, "12500000", byAsset["USDT"].Amount.String())
	require.Equal(t, mintTx["transactionHash"], byAsset["USDT"].TxID)

	d.mine(12)
	_, err = s.Step(ctx)
	require.NoError(t, err)
	ds, err = st.ListDeposits(ctx, 5, 0, 10)
	require.NoError(t, err)
	for _, dp := range ds {
		require.Equal(t, store.StatusConfirmed, dp.Status, dp.Asset)
		require.GreaterOrEqual(t, dp.Confirmations, int64(12))
	}
}
