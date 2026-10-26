// Package btc reads blocks from a bitcoind node over JSON-RPC and implements
// scan.Chain. Output amounts are taken from the JSON as decimal strings and
// parsed into satoshis exactly; they never pass through a float (ADR 0001).
package btc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/pkg/money"
)

// Client is a minimal bitcoind JSON-RPC client.
type Client struct {
	URL, User, Password string
	HTTP                *http.Client
	id                  atomic.Int64
}

// NewClient returns a client for url (e.g. http://bitcoind:18443).
func NewClient(url, user, password string) *Client {
	return &Client{URL: url, User: user, Password: password, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// RPCError is an error returned by bitcoind.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("bitcoind rpc error %d: %s", e.Code, e.Message) }

// rpcInvalidParameter is bitcoind's RPC_INVALID_PARAMETER (e.g. height out of range).
const rpcInvalidParameter = -8

// Call invokes method and decodes the result into out (may be nil).
func (c *Client) Call(ctx context.Context, method string, out any, params ...any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "1.0", "id": c.id.Add(1), "method": method, "params": params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.User, c.Password)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return err
	}
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return fmt.Errorf("bitcoind %s: http %d: %w", method, resp.StatusCode, err)
	}
	if env.Error != nil {
		return env.Error
	}
	if out == nil {
		return nil
	}
	dec = json.NewDecoder(bytes.NewReader(env.Result))
	dec.UseNumber()
	return dec.Decode(out)
}

// Chain implements scan.Chain over a bitcoind node.
type Chain struct{ C *Client }

var _ scan.Chain = Chain{}

// TipHeight implements scan.Chain.
func (ch Chain) TipHeight(ctx context.Context) (int64, error) {
	var n int64
	err := ch.C.Call(ctx, "getblockcount", &n)
	return n, err
}

type rawBlock struct {
	Hash   string `json:"hash"`
	Height int64  `json:"height"`
	Prev   string `json:"previousblockhash"`
	Tx     []struct {
		TxID string `json:"txid"`
		Vout []struct {
			Value        json.Number `json:"value"`
			N            int64       `json:"n"`
			ScriptPubKey struct {
				Address string `json:"address"`
			} `json:"scriptPubKey"`
		} `json:"vout"`
	} `json:"tx"`
}

// BlockAt implements scan.Chain. Every output paying an address becomes a
// transfer; the store keeps those paying watched deposit addresses.
func (ch Chain) BlockAt(ctx context.Context, height int64) (scan.Block, error) {
	var hash string
	if err := ch.C.Call(ctx, "getblockhash", &hash, height); err != nil {
		var re *RPCError
		if errors.As(err, &re) && re.Code == rpcInvalidParameter {
			return scan.Block{}, scan.ErrNoBlock
		}
		return scan.Block{}, err
	}
	var rb rawBlock
	if err := ch.C.Call(ctx, "getblock", &rb, hash, 2); err != nil {
		return scan.Block{}, err
	}
	return toBlock(rb)
}

func toBlock(rb rawBlock) (scan.Block, error) {
	b := scan.Block{Height: rb.Height, Hash: rb.Hash, Parent: rb.Prev}
	for _, tx := range rb.Tx {
		for _, o := range tx.Vout {
			if o.ScriptPubKey.Address == "" {
				continue // OP_RETURN, bare multisig, ...
			}
			sats, err := money.BTC.Parse(o.Value.String())
			if err != nil {
				return scan.Block{}, fmt.Errorf("block %s tx %s vout %d: amount %q: %w", rb.Hash, tx.TxID, o.N, o.Value, err)
			}
			b.Transfers = append(b.Transfers, scan.Transfer{TxID: tx.TxID, Index: o.N, Address: o.ScriptPubKey.Address, Asset: money.BTC.Symbol, Amount: sats})
		}
	}
	return b, nil
}
