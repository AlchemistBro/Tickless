package btc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/chainwatcher/internal/scan"
)

// A getblock (verbosity 2) result trimmed to the fields we read. Amounts are
// written exactly as bitcoind prints them (fixed 8 decimals).
const blockJSON = `{
  "hash": "00h", "height": 105, "previousblockhash": "00p",
  "tx": [
    {"txid": "cb", "vout": [{"value": 50.00000000, "n": 0, "scriptPubKey": {"address": "bcrt1qminer"}}]},
    {"txid": "t1", "vout": [
      {"value": 0.00000001, "n": 0, "scriptPubKey": {"address": "bcrt1qalice"}},
      {"value": 20999999.97690000, "n": 1, "scriptPubKey": {"address": "bcrt1qbob"}},
      {"value": 0.00000000, "n": 2, "scriptPubKey": {"type": "nulldata"}}
    ]}
  ]
}`

func TestToBlockParsesExactSatoshis(t *testing.T) {
	var rb rawBlock
	dec := json.NewDecoder(strings.NewReader(blockJSON))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&rb))
	b, err := toBlock(rb)
	require.NoError(t, err)
	require.Equal(t, int64(105), b.Height)
	require.Equal(t, "00h", b.Hash)
	require.Equal(t, "00p", b.Parent)
	require.Len(t, b.Transfers, 3, "the OP_RETURN output has no address")

	got := map[string]string{}
	for _, tr := range b.Transfers {
		require.Equal(t, "BTC", tr.Asset)
		got[tr.Address] = tr.Amount.String()
	}
	require.Equal(t, map[string]string{
		"bcrt1qminer": "5000000000",
		"bcrt1qalice": "1",
		"bcrt1qbob":   "2099999997690000", // 20999999.9769 has no exact float64 representation
	}, got)
	require.Equal(t, scan.Transfer{TxID: "t1", Index: 1, Address: "bcrt1qbob", Asset: "BTC", Amount: b.Transfers[2].Amount}, b.Transfers[2])
}

func TestToBlockRejectsAmountsItCannotRepresentExactly(t *testing.T) {
	var rb rawBlock
	dec := json.NewDecoder(strings.NewReader(`{"hash":"h","height":1,"tx":[{"txid":"t","vout":[{"value":1.000000001,"n":0,"scriptPubKey":{"address":"a"}}]}]}`))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&rb))
	_, err := toBlock(rb)
	require.Error(t, err, "9 decimals is not a valid BTC amount; it must not be rounded silently")
}

func TestBlockAtAboveTipIsErrNoBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.Contains(t, string(body), `"getblockhash"`)
		user, pass, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "u:p", user+":"+pass)
		_, _ = w.Write([]byte(`{"result":null,"error":{"code":-8,"message":"Block height out of range"},"id":1}`))
	}))
	defer srv.Close()
	_, err := Chain{C: NewClient(srv.URL, "u", "p")}.BlockAt(context.Background(), 999)
	require.ErrorIs(t, err, scan.ErrNoBlock)
}

func TestRPCErrorsAreReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":null,"error":{"code":-28,"message":"Loading block index..."},"id":1}`))
	}))
	defer srv.Close()
	_, err := Chain{C: NewClient(srv.URL, "u", "p")}.TipHeight(context.Background())
	var re *RPCError
	require.ErrorAs(t, err, &re)
	require.Equal(t, -28, re.Code)
}
