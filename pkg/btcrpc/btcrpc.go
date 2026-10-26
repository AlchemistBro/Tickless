// Package btcrpc is a minimal bitcoind JSON-RPC client. Numbers are decoded as
// json.Number so amounts never pass through a float (ADR 0001).
package btcrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Error is an error returned by bitcoind.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("bitcoind rpc error %d: %s", e.Code, e.Message) }

// Well-known bitcoind error codes.
const (
	CodeInvalidAddressOrKey  = -5  // e.g. "No such mempool or blockchain transaction"
	CodeVerifyRejected       = -26 // rejected by policy / mempool
	CodeVerifyAlreadyInChain = -27
)

// Client calls one bitcoind endpoint (append /wallet/<name> for wallet RPCs).
type Client struct {
	URL, User, Password string
	HTTP                *http.Client
	id                  atomic.Int64
}

// New returns a client.
func New(url, user, password string) *Client {
	return &Client{URL: url, User: user, Password: password, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

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
		Error  *Error          `json:"error"`
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
