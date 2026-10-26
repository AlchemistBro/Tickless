package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/pkg/btcrpc"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	signerv1 "github.com/tickless/pkg/gen/tickless/signer/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/rabbit"
	"github.com/tickless/withdrawals/internal/engine"
	"github.com/tickless/withdrawals/internal/store"
)

// ---------------------------------------------------------------- ledger

type ledgerAdapter struct{ c ledgerv1.LedgerServiceClient }

func (l ledgerAdapter) HoldWithdrawal(ctx context.Context, id, user int64, asset string, amount money.Int128) error {
	_, err := l.c.HoldWithdrawal(ctx, &ledgerv1.HoldWithdrawalRequest{WithdrawalId: id, UserId: user, Asset: asset, Amount: &commonv1.Amount{Units: amount.String()}})
	if status.Code(err) == codes.FailedPrecondition {
		return engine.ErrInsufficientFunds
	}
	return err
}

func (l ledgerAdapter) SettleWithdrawal(ctx context.Context, id int64) error {
	_, err := l.c.SettleWithdrawal(ctx, &ledgerv1.SettleWithdrawalRequest{WithdrawalId: id})
	return err
}

func (l ledgerAdapter) ReleaseWithdrawal(ctx context.Context, id int64) error {
	_, err := l.c.ReleaseWithdrawal(ctx, &ledgerv1.ReleaseWithdrawalRequest{WithdrawalId: id})
	if status.Code(err) == codes.NotFound {
		return engine.ErrNoHold
	}
	return err
}

// ---------------------------------------------------------------- signer (mTLS)

type signerAdapter struct{ c signerv1.SignerServiceClient }

func (s signerAdapter) Sign(ctx context.Context, w store.Withdrawal) (engine.Signed, error) {
	r, err := s.c.SignWithdrawal(ctx, &signerv1.SignWithdrawalRequest{WithdrawalId: w.ID, Chain: w.Chain, Asset: w.Asset,
		Amount: &commonv1.Amount{Units: w.Amount.String()}, Destination: w.Destination})
	switch status.Code(err) {
	case codes.OK:
		return engine.Signed{TxID: r.Txid, Raw: r.RawTx, NetworkFee: r.GetNetworkFee().GetUnits(), FeeAsset: r.FeeAsset}, nil
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unknown:
		return engine.Signed{}, fmt.Errorf("%w: %w", engine.ErrRetry, err)
	default:
		return engine.Signed{}, err // InvalidArgument / FailedPrecondition / PermissionDenied: final
	}
}

// ---------------------------------------------------------------- identity

type verifier struct {
	c identityv1.IdentityServiceClient
}

func (v verifier) VerifyTOTP(ctx context.Context, user int64, code string) error {
	_, err := v.c.VerifyTOTP(ctx, &identityv1.VerifyTOTPRequest{UserId: user, Code: code})
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.Unauthenticated:
		return engine.ErrTOTPInvalid
	case codes.NotFound:
		return engine.ErrTOTPRequired
	default:
		return err
	}
}

// ---------------------------------------------------------------- RabbitMQ

// Queue names (ADR 0011).
const (
	QueueScreening = "withdrawals.screening"
	QueueReview    = "withdrawals.review"
)

type rabbitQueue struct{ c *rabbit.Client }

func (q rabbitQueue) PublishScreening(ctx context.Context, id int64) error {
	return q.c.Publish(ctx, QueueScreening, []byte(fmt.Sprint(id)), nil)
}

func (q rabbitQueue) PublishReview(ctx context.Context, w store.Withdrawal) error {
	b, err := proto.Marshal(&withdrawalsv1.WithdrawalEvent{WithdrawalId: w.ID, UserId: w.UserID, Chain: w.Chain, Asset: w.Asset,
		Amount: &commonv1.Amount{Units: w.Amount.String()}, Destination: w.Destination, ToState: string(w.State)})
	if err != nil {
		return err
	}
	return q.c.Publish(ctx, QueueReview, b, nil)
}

// ---------------------------------------------------------------- bitcoind

type btcChain struct{ c *btcrpc.Client }

func (b btcChain) Broadcast(ctx context.Context, raw []byte, txid string) error {
	err := b.c.Call(ctx, "sendrawtransaction", nil, hex.EncodeToString(raw))
	var re *btcrpc.Error
	if err == nil || (errors.As(err, &re) && (re.Code == btcrpc.CodeVerifyAlreadyInChain ||
		strings.Contains(re.Message, "already-in-mempool") || strings.Contains(re.Message, "txn-already-known"))) {
		return nil
	}
	// Inputs already spent can mean "spent by this very transaction": ask the node.
	if confs, _, cerr := b.Confirmations(ctx, txid); cerr == nil && confs >= 0 {
		return nil
	}
	return err
}

func (b btcChain) Confirmations(ctx context.Context, txid string) (int64, bool, error) {
	var tx struct {
		Confirmations json.Number `json:"confirmations"`
	}
	err := b.c.Call(ctx, "getrawtransaction", &tx, txid, true)
	var re *btcrpc.Error
	if errors.As(err, &re) && re.Code == btcrpc.CodeInvalidAddressOrKey {
		return -1, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if tx.Confirmations == "" {
		return 0, false, nil // in the mempool
	}
	n, err := tx.Confirmations.Int64()
	return n, false, err
}

// ---------------------------------------------------------------- Ethereum

type ethChain struct{ c *rpc.Client }

func (e ethChain) Broadcast(ctx context.Context, raw []byte, txid string) error {
	var h common.Hash
	err := e.c.CallContext(ctx, &h, "eth_sendRawTransaction", hexutil.Bytes(raw))
	if err == nil {
		return nil
	}
	if confs, _, cerr := e.Confirmations(ctx, txid); cerr == nil && confs >= 0 {
		return nil // "already known" / "nonce too low" because it is already there
	}
	return err
}

func (e ethChain) Confirmations(ctx context.Context, txid string) (int64, bool, error) {
	var rcpt *struct {
		Status      hexutil.Uint64 `json:"status"`
		BlockNumber *hexutil.Big   `json:"blockNumber"`
	}
	if err := e.c.CallContext(ctx, &rcpt, "eth_getTransactionReceipt", common.HexToHash(txid)); err != nil {
		return 0, false, err
	}
	if rcpt == nil || rcpt.BlockNumber == nil {
		var tx map[string]any
		if err := e.c.CallContext(ctx, &tx, "eth_getTransactionByHash", common.HexToHash(txid)); err != nil {
			return 0, false, err
		}
		if tx == nil {
			return -1, false, nil
		}
		return 0, false, nil
	}
	if rcpt.Status != 1 {
		return 0, true, nil
	}
	var head hexutil.Big
	if err := e.c.CallContext(ctx, &head, "eth_blockNumber"); err != nil {
		return 0, false, err
	}
	confs := new(big.Int).Sub(head.ToInt(), rcpt.BlockNumber.ToInt())
	return confs.Int64() + 1, false, nil
}
