//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	btcRPCURL = env("E2E_BTC_RPC", "http://localhost:18443")
	ethRPCURL = env("E2E_ETH_RPC", "http://localhost:8545")
	// Test USDT on the devnet (deploy/compose/chains/eth-init.sh) and Anvil dev account 0.
	usdtToken = env("E2E_USDT_CONTRACT", "0x5FbDB2315678afecb367f032d93F642f64180aa3")
	ethFunder = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
)

// creditTarget is SPEC §5: a deposit is credited <= 10 s after the confirmation threshold.
const creditTarget = 10 * time.Second

// rpc calls a JSON-RPC 1.0/2.0 endpoint (bitcoind or Anvil) and decodes the result.
func rpc(t *testing.T, url, method string, out any, params ...any) {
	t.Helper()
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(url, btcRPCURL) {
		req.SetBasicAuth("tickless", "tickless") // local regtest credentials
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	dec := json.NewDecoder(res.Body)
	dec.UseNumber()
	require.NoError(t, dec.Decode(&env), method)
	require.Nil(t, env.Error, "%s: %+v", method, env.Error)
	if out != nil {
		require.NoError(t, json.Unmarshal(env.Result, out), method)
	}
}

type btcNode struct {
	t      *testing.T
	faucet string // address of the faucet wallet (block rewards)
	fork   string // coinbase address for alternative branches
}

func newBTC(t *testing.T) *btcNode {
	t.Helper()
	n := &btcNode{t: t}
	rpc(t, btcRPCURL+"/wallet/faucet", "getnewaddress", &n.faucet, "", "bech32")
	rpc(t, btcRPCURL+"/wallet/faucet", "getnewaddress", &n.fork, "", "bech32")
	return n
}

func (n *btcNode) mine(k int) { rpc(n.t, btcRPCURL, "generatetoaddress", nil, k, n.faucet) }
func (n *btcNode) height() int64 {
	var h int64
	rpc(n.t, btcRPCURL, "getblockcount", &h)
	return h
}
func (n *btcNode) send(addr, btc string) string {
	var txid string
	rpc(n.t, btcRPCURL+"/wallet/faucet", "sendtoaddress", &txid, addr, btc)
	return txid
}

type deposit struct {
	ID                    string `json:"id"`
	Chain                 string `json:"chain"`
	TxID                  string `json:"txid"`
	Index                 int64  `json:"index"`
	Asset                 string `json:"asset"`
	Amount                string `json:"amount"`
	Status                string `json:"status"`
	Confirmations         int64  `json:"confirmations"`
	RequiredConfirmations int64  `json:"required_confirmations"`
}

func (u *user) depositAddress(chain string) string {
	u.t.Helper()
	r := call(u.t, "GET", "/v1/deposit-address?chain="+chain, u.Access, nil, nil)
	require.Equal(u.t, 200, r.Status, string(r.Body))
	var b struct{ Address string }
	r.json(u.t, &b)
	require.NotEmpty(u.t, b.Address)
	return b.Address
}

func (u *user) deposits() []deposit {
	u.t.Helper()
	r := call(u.t, "GET", "/v1/deposits", u.Access, nil, nil)
	require.Equal(u.t, 200, r.Status, string(r.Body))
	var b struct{ Deposits []deposit }
	r.json(u.t, &b)
	return b.Deposits
}

// waitDeposit polls until the deposit of txid satisfies cond.
func (u *user) waitDeposit(txid string, cond func(deposit) bool, what string) deposit {
	u.t.Helper()
	var last deposit
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range u.deposits() {
			if strings.EqualFold(d.TxID, txid) {
				last = d
				if cond(d) {
					return d
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	u.t.Fatalf("deposit %s never became %s; last %+v", txid, what, last)
	return deposit{}
}

// waitCredit returns how long after `since` the balance reached want.
func (u *user) waitCredit(asset, want string, since time.Time) time.Duration {
	u.t.Helper()
	u.waitBalance(asset, want, "0")
	return time.Since(since)
}

func depositEntries(t *testing.T, chain, txid string) int {
	t.Helper()
	var n int
	require.NoError(t, ledgerDB(t).QueryRow(context.Background(),
		`SELECT count(*) FROM journal_entries WHERE business_key LIKE $1`, fmt.Sprintf("deposit:%s:%s:%%", chain, txid)).Scan(&n))
	return n
}

// Sign up -> deposit address -> regtest payment -> pending below the
// threshold -> credited within SPEC §5's 10 s after the threshold.
func TestBTCDepositIsCreditedAfterTwoConfirmations(t *testing.T) {
	u := newUser(t, "btcdep")
	addr := u.depositAddress("btc")
	require.True(t, strings.HasPrefix(addr, "bcrt1q"), addr)
	require.Equal(t, addr, u.depositAddress("btc"), "the deposit address is stable")

	n := newBTC(t)
	txid := n.send(addr, "0.12345678")
	n.mine(1)
	d := u.waitDeposit(txid, func(d deposit) bool { return d.Confirmations == 1 }, "seen with 1 confirmation")
	require.Equal(t, "pending", d.Status)
	require.Equal(t, "0.12345678", d.Amount)
	require.EqualValues(t, 2, d.RequiredConfirmations)
	time.Sleep(2 * time.Second)
	require.Empty(t, u.balances()["BTC"].Available, "nothing is credited below the threshold")

	n.mine(1)
	threshold := time.Now()
	lag := u.waitCredit("BTC", "0.12345678", threshold)
	t.Logf("BTC deposit credited %v after the confirmation threshold (target %v)", lag, creditTarget)
	require.LessOrEqual(t, lag, creditTarget, "SPEC §5 deposit credit target")
	u.waitDeposit(txid, func(d deposit) bool { return d.Status == "confirmed" }, "confirmed")
	require.Equal(t, 1, depositEntries(t, "btc", txid))
	assertLedgerInvariants(t)
}

// SPEC §7 chaos "force a 3-block reorg on regtest" against the running stack:
// the removed deposit is never credited, and when the transaction is mined
// again on the new branch it is credited exactly once.
func TestForcedThreeBlockReorgNeverCreditsAndRecovers(t *testing.T) {
	u := newUser(t, "reorg")
	addr := u.depositAddress("btc")
	n := newBTC(t)
	n.mine(2)
	txid := n.send(addr, "0.5")
	n.mine(1)
	u.waitDeposit(txid, func(d deposit) bool { return d.Status == "pending" && d.Confirmations == 1 }, "pending")

	var fork string
	rpc(t, btcRPCURL, "getblockhash", &fork, n.height()-2)
	rpc(t, btcRPCURL, "invalidateblock", nil, fork)
	for i := 0; i < 4; i++ { // a longer branch without the transaction
		rpc(t, btcRPCURL, "generateblock", nil, n.fork, []string{})
	}
	u.waitDeposit(txid, func(d deposit) bool { return d.Status == "reorged" }, "reorged")
	for i := 0; i < 3; i++ { // far past the threshold on the new branch
		rpc(t, btcRPCURL, "generateblock", nil, n.fork, []string{})
	}
	time.Sleep(3 * time.Second)
	require.Empty(t, u.balances()["BTC"].Available, "a reorged deposit is never credited")
	require.Zero(t, depositEntries(t, "btc", txid))

	// Recovery: the sender rebroadcasts; normal blocks mine it on the new branch.
	var wtx struct{ Hex string }
	rpc(t, btcRPCURL+"/wallet/faucet", "gettransaction", &wtx, txid)
	rpc(t, btcRPCURL, "sendrawtransaction", nil, wtx.Hex)
	n.mine(2)
	u.waitCredit("BTC", "0.5", time.Now())
	require.Equal(t, 1, depositEntries(t, "btc", txid), "credited exactly once")
	require.Len(t, u.deposits(), 1, "same (txid, vout), same deposit")
	assertLedgerInvariants(t)
}

func hexWord(v *big.Int) string { return fmt.Sprintf("%064x", v) }

// Native ETH and ERC-20 Test USDT to the same Ethereum deposit address.
func TestETHAndUSDTDepositsAreCreditedAfterTwelveConfirmations(t *testing.T) {
	u := newUser(t, "ethdep")
	addr := u.depositAddress("eth")
	require.Regexp(t, `^0x[0-9a-fA-F]{40}$`, addr)

	var ethTx, usdtTx string
	rpc(t, ethRPCURL, "eth_sendTransaction", &ethTx, map[string]any{"from": ethFunder, "to": addr, "value": "0x" + big.NewInt(250_000_000_000_000_000).Text(16)})
	// mint(address,uint256) = 0x40c10f19
	data := "0x40c10f19" + hexWord(new(big.Int).SetBytes(mustHex(t, addr[2:]))) + hexWord(big.NewInt(100_500_000))
	rpc(t, ethRPCURL, "eth_sendTransaction", &usdtTx, map[string]any{"from": ethFunder, "to": usdtToken, "data": data})
	u.waitDeposit(ethTx, func(d deposit) bool { return d.Asset == "ETH" }, "seen")
	d := u.waitDeposit(usdtTx, func(d deposit) bool { return d.Asset == "USDT" }, "seen")
	require.Equal(t, "100.5", d.Amount)
	require.EqualValues(t, 12, d.RequiredConfirmations)

	rpc(t, ethRPCURL, "anvil_mine", nil, "0xc") // 12 blocks: past the threshold of both
	threshold := time.Now()
	lag := u.waitCredit("ETH", "0.25", threshold)
	u.waitCredit("USDT", "100.5", threshold)
	t.Logf("ETH deposit credited %v after the confirmation threshold (target %v)", lag, creditTarget)
	require.LessOrEqual(t, lag, creditTarget, "SPEC §5 deposit credit target")
	require.Equal(t, 1, depositEntries(t, "eth", ethTx))
	require.Equal(t, 1, depositEntries(t, "eth", usdtTx))
	assertLedgerInvariants(t)
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
