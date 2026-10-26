// Package server implements the signer's gRPC API. It is served only over
// mutual TLS to the withdrawals service (see app). Every request is idempotent
// by withdrawal id through the journal: the first signature is recorded before
// it is returned, and a retry gets exactly that transaction back.
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	signerv1 "github.com/tickless/pkg/gen/tickless/signer/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/signer/internal/btcsign"
	"github.com/tickless/signer/internal/ethsign"
	"github.com/tickless/signer/internal/journal"
	"github.com/tickless/signer/internal/keys"
)

// Server signs withdrawals.
type Server struct {
	signerv1.UnimplementedSignerServiceServer
	Wallet     *keys.Wallet
	Journal    *journal.Journal
	BTC        *BitcoindRPC   // nil disables BTC
	ETH        *rpc.Client    // nil disables ETH
	Token      common.Address // Test USDT contract
	BTCFeeRate int64          // sat/vB
	ETHTip     *big.Int       // wei

	btcMu, ethMu sync.Mutex
}

// HotWallet implements signerv1.SignerServiceServer.
func (s *Server) HotWallet(_ context.Context, r *signerv1.HotWalletRequest) (*signerv1.HotWalletResponse, error) {
	switch r.Chain {
	case "btc":
		a, err := s.Wallet.BTCAddress()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &signerv1.HotWalletResponse{Address: a.EncodeAddress()}, nil
	case "eth":
		return &signerv1.HotWalletResponse{Address: s.Wallet.ETHAddress().Hex()}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown chain %q", r.Chain)
	}
}

// SignWithdrawal implements signerv1.SignerServiceServer.
func (s *Server) SignWithdrawal(ctx context.Context, r *signerv1.SignWithdrawalRequest) (*signerv1.SignWithdrawalResponse, error) {
	amt, err := money.ParseInt128(r.GetAmount().GetUnits())
	if err != nil || amt.Sign() <= 0 || r.WithdrawalId <= 0 || r.Destination == "" {
		return nil, status.Error(codes.InvalidArgument, "withdrawal_id, a positive amount and a destination are required")
	}
	switch {
	case r.Chain == "btc" && r.Asset == "BTC" && s.BTC != nil:
		s.btcMu.Lock()
		defer s.btcMu.Unlock()
	case r.Chain == "eth" && (r.Asset == "ETH" || r.Asset == "USDT") && s.ETH != nil:
		s.ethMu.Lock()
		defer s.ethMu.Unlock()
	default:
		return nil, status.Errorf(codes.InvalidArgument, "cannot pay %s on %s", r.Asset, r.Chain)
	}
	if rec, ok, err := s.Journal.Get(r.WithdrawalId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	} else if ok {
		if !rec.SameTerms(r.Chain, r.Asset, amt.String(), r.Destination) {
			return nil, status.Errorf(codes.FailedPrecondition, "withdrawal %d was already signed with different terms", r.WithdrawalId)
		}
		return response(rec)
	}
	var rec journal.Record
	if r.Chain == "btc" {
		rec, err = s.signBTC(ctx, r, amt)
	} else {
		rec, err = s.signETH(ctx, r, amt)
	}
	if err != nil {
		return nil, err
	}
	if err := s.Journal.Put(rec); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	slog.InfoContext(ctx, "withdrawal signed", "withdrawal", r.WithdrawalId, "chain", r.Chain, "asset", r.Asset, "txid", rec.TxID)
	return response(rec)
}

func response(rec journal.Record) (*signerv1.SignWithdrawalResponse, error) {
	raw, err := hex.DecodeString(rec.RawTx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &signerv1.SignWithdrawalResponse{Txid: rec.TxID, RawTx: raw,
		NetworkFee: &commonv1.Amount{Units: rec.NetworkFee}, FeeAsset: rec.FeeAsset}, nil
}

func (s *Server) signBTC(ctx context.Context, r *signerv1.SignWithdrawalRequest, amt money.Int128) (journal.Record, error) {
	dest, err := btcutil.DecodeAddress(r.Destination, s.Wallet.Params)
	if err != nil || !dest.IsForNet(s.Wallet.Params) {
		return journal.Record{}, status.Errorf(codes.InvalidArgument, "invalid BTC destination %q", r.Destination)
	}
	sats, ok := amt.Int64()
	if !ok {
		return journal.Record{}, status.Error(codes.InvalidArgument, "amount out of range")
	}
	from, err := s.Wallet.BTCAddress()
	if err != nil {
		return journal.Record{}, status.Error(codes.Internal, err.Error())
	}
	all, err := s.BTC.UTXOs(ctx, from.EncodeAddress())
	if err != nil {
		return journal.Record{}, status.Errorf(codes.Unavailable, "bitcoind: %v", err)
	}
	spent, err := s.Journal.SpentOutpoints()
	if err != nil {
		return journal.Record{}, status.Error(codes.Internal, err.Error())
	}
	var free []btcsign.UTXO
	for _, u := range all {
		if !spent[u.Outpoint()] {
			free = append(free, u)
		}
	}
	res, err := btcsign.Build(s.Wallet.BTC, from, dest, free, sats, s.BTCFeeRate)
	if errors.Is(err, btcsign.ErrInsufficient) {
		// Not final: confirmed change or a top-up makes it payable later.
		return journal.Record{}, status.Error(codes.Unavailable, err.Error())
	}
	if err != nil {
		return journal.Record{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return journal.Record{WithdrawalID: r.WithdrawalId, Chain: "btc", Asset: "BTC", Amount: amt.String(), Destination: r.Destination,
		TxID: res.TxID, RawTx: hex.EncodeToString(res.Raw), NetworkFee: fmt.Sprint(res.Fee), FeeAsset: "BTC", Spent: res.Spent}, nil
}

func (s *Server) signETH(ctx context.Context, r *signerv1.SignWithdrawalRequest, amt money.Int128) (journal.Record, error) {
	if !common.IsHexAddress(r.Destination) {
		return journal.Record{}, status.Errorf(codes.InvalidArgument, "invalid ETH destination %q", r.Destination)
	}
	dest := common.HexToAddress(r.Destination)
	hot := s.Wallet.ETHAddress()
	var chainID hexutil.Big
	var nonce hexutil.Uint64
	if err := s.ETH.CallContext(ctx, &chainID, "eth_chainId"); err != nil {
		return journal.Record{}, status.Errorf(codes.Unavailable, "eth: %v", err)
	}
	if err := s.ETH.CallContext(ctx, &nonce, "eth_getTransactionCount", hot, "pending"); err != nil {
		return journal.Record{}, status.Errorf(codes.Unavailable, "eth: %v", err)
	}
	next, err := s.Journal.NextNonce("eth")
	if err != nil {
		return journal.Record{}, status.Error(codes.Internal, err.Error())
	}
	n := max(uint64(nonce), next)
	var head struct {
		BaseFee *hexutil.Big `json:"baseFeePerGas"`
	}
	if err := s.ETH.CallContext(ctx, &head, "eth_getBlockByNumber", "latest", false); err != nil {
		return journal.Record{}, status.Errorf(codes.Unavailable, "eth: %v", err)
	}
	base := big.NewInt(1_000_000_000)
	if head.BaseFee != nil {
		base = head.BaseFee.ToInt()
	}
	tip := s.ETHTip
	if tip == nil {
		tip = big.NewInt(1_000_000_000)
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tip)
	p := ethsign.Params{ChainID: chainID.ToInt(), Nonce: n, GasTipCap: tip, GasFeeCap: feeCap}
	var token *common.Address
	if r.Asset == "USDT" {
		token = &s.Token
		var gas hexutil.Uint64
		call := map[string]any{"from": hot, "to": s.Token, "data": hexutil.Bytes(ethsign.TransferData(dest, amt.Big()))}
		if err := s.ETH.CallContext(ctx, &gas, "eth_estimateGas", call); err != nil {
			// Typically the hot wallet lacks the tokens; retryable after a top-up.
			return journal.Record{}, status.Errorf(codes.Unavailable, "estimate token transfer: %v", err)
		}
		p.Gas = uint64(gas) * 5 / 4
	}
	signed, err := ethsign.Payment(s.Wallet.ETH, token, dest, amt.Big(), p)
	if err != nil {
		return journal.Record{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return journal.Record{WithdrawalID: r.WithdrawalId, Chain: "eth", Asset: r.Asset, Amount: amt.String(), Destination: r.Destination,
		TxID: signed.TxID, RawTx: hex.EncodeToString(signed.Raw), NetworkFee: signed.MaxFee.String(), FeeAsset: "ETH", Nonce: &n}, nil
}

// BitcoindRPC is the minimal bitcoind JSON-RPC client the signer needs.
type BitcoindRPC struct {
	URL, User, Password string
	HTTP                *http.Client
	id                  atomic.Int64
}

// NewBitcoindRPC returns a client.
func NewBitcoindRPC(url, user, password string) *BitcoindRPC {
	return &BitcoindRPC{URL: url, User: user, Password: password, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// Call invokes method and decodes the result (numbers kept as json.Number).
func (b *BitcoindRPC) Call(ctx context.Context, method string, out any, params ...any) error {
	if params == nil {
		params = []any{}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "1.0", "id": b.id.Add(1), "method": method, "params": params})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(b.User, b.Password)
	resp, err := b.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return fmt.Errorf("bitcoind %s: %w", method, err)
	}
	if env.Error != nil {
		return fmt.Errorf("bitcoind %s: %d %s", method, env.Error.Code, env.Error.Message)
	}
	if out == nil {
		return nil
	}
	dec = json.NewDecoder(bytes.NewReader(env.Result))
	dec.UseNumber()
	return dec.Decode(out)
}

// UTXOs returns the confirmed unspent outputs paying address (scantxoutset).
func (b *BitcoindRPC) UTXOs(ctx context.Context, address string) ([]btcsign.UTXO, error) {
	var res struct {
		Unspents []struct {
			TxID         string      `json:"txid"`
			Vout         uint32      `json:"vout"`
			ScriptPubKey string      `json:"scriptPubKey"`
			Amount       json.Number `json:"amount"`
		} `json:"unspents"`
	}
	if err := b.Call(ctx, "scantxoutset", &res, "start", []string{"addr(" + address + ")"}); err != nil {
		return nil, err
	}
	out := make([]btcsign.UTXO, 0, len(res.Unspents))
	for _, u := range res.Unspents {
		v, err := money.BTC.Parse(normalizeBTC(u.Amount.String()))
		if err != nil {
			return nil, fmt.Errorf("utxo %s:%d amount %q: %w", u.TxID, u.Vout, u.Amount, err)
		}
		sats, ok := v.Int64()
		if !ok {
			return nil, fmt.Errorf("utxo %s:%d amount out of range", u.TxID, u.Vout)
		}
		script, err := hex.DecodeString(u.ScriptPubKey)
		if err != nil {
			return nil, err
		}
		out = append(out, btcsign.UTXO{TxID: u.TxID, Vout: u.Vout, Value: sats, PkScript: script})
	}
	return out, nil
}

// normalizeBTC trims insignificant trailing zeros beyond 8 decimals, should a
// node ever print them; more than 8 significant decimals still fail to parse.
func normalizeBTC(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 && len(s)-i-1 > 8 {
		return strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	}
	return s
}
