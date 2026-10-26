// Package eth reads blocks from an Ethereum JSON-RPC node (Anvil locally) and
// implements scan.Chain: native ETH value transfers and ERC-20 Transfer logs
// of one configured token.
//
// Block and parent hashes are taken from the node's JSON as they are, never
// recomputed from a decoded header, so the reorg check compares exactly what
// the node reports. Only successful transactions (receipt status 1) count: a
// reverted transaction moves no value and emits no logs.
//
// Deliberate simplification (ADR 0010): ETH sent to a deposit address by a
// contract (an internal call) is not detected; only top-level transactions.
package eth

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/pkg/money"
)

// NativeIndex is the deposit index of a transaction's own value transfer.
const NativeIndex int64 = -1

// TransferTopic is keccak256("Transfer(address,address,uint256)").
var TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// Chain implements scan.Chain over an Ethereum node.
type Chain struct {
	RPC        *rpc.Client
	Token      common.Address // ERC-20 contract to follow; zero = none
	TokenAsset string         // asset symbol of the token, e.g. "USDT"
}

var _ scan.Chain = Chain{}

// TipHeight implements scan.Chain.
func (c Chain) TipHeight(ctx context.Context) (int64, error) {
	var n hexutil.Uint64
	if err := c.RPC.CallContext(ctx, &n, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return int64(n), nil //nolint:gosec // block numbers fit in int64
}

type rpcTx struct {
	Hash  common.Hash     `json:"hash"`
	To    *common.Address `json:"to"`
	Value *hexutil.Big    `json:"value"`
}

type rpcBlock struct {
	Number       hexutil.Uint64 `json:"number"`
	Hash         common.Hash    `json:"hash"`
	ParentHash   common.Hash    `json:"parentHash"`
	Transactions []rpcTx        `json:"transactions"`
}

type rpcLog struct {
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	LogIndex hexutil.Uint64 `json:"logIndex"`
	Removed  bool           `json:"removed"`
}

type rpcReceipt struct {
	TxHash common.Hash    `json:"transactionHash"`
	Status hexutil.Uint64 `json:"status"`
	Logs   []rpcLog       `json:"logs"`
}

// BlockAt implements scan.Chain.
func (c Chain) BlockAt(ctx context.Context, height int64) (scan.Block, error) {
	var b *rpcBlock
	if err := c.RPC.CallContext(ctx, &b, "eth_getBlockByNumber", hexutil.EncodeBig(big.NewInt(height)), true); err != nil {
		return scan.Block{}, err
	}
	if b == nil {
		return scan.Block{}, scan.ErrNoBlock
	}
	var receipts []rpcReceipt
	if len(b.Transactions) > 0 {
		// By hash: the receipts belong to exactly this block even if the chain
		// reorganises between the two calls.
		if err := c.RPC.CallContext(ctx, &receipts, "eth_getBlockReceipts", b.Hash.Hex()); err != nil {
			return scan.Block{}, err
		}
		if len(receipts) != len(b.Transactions) {
			return scan.Block{}, fmt.Errorf("block %s: %d receipts for %d transactions", b.Hash, len(receipts), len(b.Transactions))
		}
	}
	return c.toBlock(b, receipts)
}

func (c Chain) toBlock(b *rpcBlock, receipts []rpcReceipt) (scan.Block, error) {
	out := scan.Block{Height: int64(b.Number), Hash: b.Hash.Hex(), Parent: b.ParentHash.Hex()} //nolint:gosec // block numbers fit in int64
	ok := make(map[common.Hash]bool, len(receipts))
	for _, r := range receipts {
		ok[r.TxHash] = r.Status == 1
	}
	for _, tx := range b.Transactions {
		if tx.To == nil || tx.Value == nil || tx.Value.ToInt().Sign() <= 0 || !ok[tx.Hash] {
			continue
		}
		amt, err := money.FromBig(tx.Value.ToInt())
		if err != nil {
			return scan.Block{}, fmt.Errorf("tx %s value: %w", tx.Hash, err)
		}
		out.Transfers = append(out.Transfers, scan.Transfer{TxID: tx.Hash.Hex(), Index: NativeIndex, Address: tx.To.Hex(), Asset: money.ETH.Symbol, Amount: amt})
	}
	if c.Token == (common.Address{}) {
		return out, nil
	}
	for _, r := range receipts {
		if r.Status != 1 {
			continue
		}
		for _, l := range r.Logs {
			if l.Removed || l.Address != c.Token || len(l.Topics) != 3 || l.Topics[0] != TransferTopic || len(l.Data) != 32 {
				continue
			}
			amt, err := money.FromBig(new(big.Int).SetBytes(l.Data))
			if err != nil {
				// A uint256 beyond the 128-bit money range cannot be a real
				// deposit of this token; refuse loudly instead of truncating.
				return scan.Block{}, fmt.Errorf("tx %s log %d amount: %w", r.TxHash, l.LogIndex, err)
			}
			to := common.BytesToAddress(l.Topics[2].Bytes())
			out.Transfers = append(out.Transfers, scan.Transfer{TxID: r.TxHash.Hex(), Index: int64(l.LogIndex), Address: to.Hex(), Asset: c.TokenAsset, Amount: amt}) //nolint:gosec // log indexes fit in int64
		}
	}
	return out, nil
}

// ParseToken parses a token contract address ("" = no token).
func ParseToken(s string) (common.Address, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return common.Address{}, nil
	}
	if !common.IsHexAddress(s) {
		return common.Address{}, fmt.Errorf("eth: invalid token address %q", s)
	}
	return common.HexToAddress(s), nil
}
