package app

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/withdrawals/internal/engine"
	"github.com/tickless/withdrawals/internal/rules"
	"github.com/tickless/withdrawals/internal/store"
)

type server struct {
	withdrawalsv1.UnimplementedWithdrawalsServiceServer
	e *engine.Engine
}

func toProto(w store.Withdrawal) *withdrawalsv1.Withdrawal {
	return &withdrawalsv1.Withdrawal{Id: w.ID, UserId: w.UserID, Chain: w.Chain, Asset: w.Asset,
		Amount: &commonv1.Amount{Units: w.Amount.String()}, Destination: w.Destination, State: string(w.State),
		Reason: w.Reason, Txid: w.TxID, Confirmations: w.Confirmations,
		CreatedAtUnixNano: w.CreatedAt.UnixNano(), UpdatedAtUnixNano: w.UpdatedAt.UnixNano()}
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rules.ErrAddress), errors.Is(err, engine.ErrUnsupported):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, rules.ErrDailyLimit), errors.Is(err, engine.ErrInsufficientFunds), errors.Is(err, engine.ErrTOTPRequired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, engine.ErrTOTPInvalid):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, store.ErrNotFound), errors.Is(err, engine.ErrNotYours):
		return status.Error(codes.NotFound, "withdrawal not found")
	case errors.Is(err, store.ErrStale):
		return status.Error(codes.FailedPrecondition, "the withdrawal is no longer in the expected state")
	default:
		slog.Error("withdrawals rpc failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

func (s *server) RequestWithdrawal(ctx context.Context, r *withdrawalsv1.RequestWithdrawalRequest) (*withdrawalsv1.RequestWithdrawalResponse, error) {
	if r.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	amt, err := money.ParseInt128(r.GetAmount().GetUnits())
	if err != nil || amt.Sign() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be a positive integer of smallest units")
	}
	w, err := s.e.Request(ctx, r.UserId, r.Chain, r.Asset, amt, r.Destination, r.TotpCode)
	if err != nil {
		return nil, mapErr(err)
	}
	return &withdrawalsv1.RequestWithdrawalResponse{Withdrawal: toProto(w)}, nil
}

func (s *server) GetWithdrawal(ctx context.Context, r *withdrawalsv1.GetWithdrawalRequest) (*withdrawalsv1.GetWithdrawalResponse, error) {
	w, err := s.e.St.Get(ctx, r.WithdrawalId)
	if err == nil && r.UserId != 0 && w.UserID != r.UserId {
		err = engine.ErrNotYours // other users' withdrawals do not exist for the caller
	}
	if err != nil {
		return nil, mapErr(err)
	}
	return &withdrawalsv1.GetWithdrawalResponse{Withdrawal: toProto(w)}, nil
}

func (s *server) ListWithdrawals(ctx context.Context, r *withdrawalsv1.ListWithdrawalsRequest) (*withdrawalsv1.ListWithdrawalsResponse, error) {
	if r.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	ws, err := s.e.St.ListForUser(ctx, r.UserId)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &withdrawalsv1.ListWithdrawalsResponse{}
	for _, w := range ws {
		out.Withdrawals = append(out.Withdrawals, toProto(w))
	}
	return out, nil
}

func (s *server) ListPendingReview(ctx context.Context, _ *withdrawalsv1.ListPendingReviewRequest) (*withdrawalsv1.ListPendingReviewResponse, error) {
	ws, err := s.e.St.ListByState(ctx, rules.PendingReview, s.farFuture(), 500)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &withdrawalsv1.ListPendingReviewResponse{}
	for _, w := range ws {
		out.Withdrawals = append(out.Withdrawals, toProto(w))
	}
	return out, nil
}

func (s *server) ReviewWithdrawal(ctx context.Context, r *withdrawalsv1.ReviewWithdrawalRequest) (*withdrawalsv1.ReviewWithdrawalResponse, error) {
	if r.AdminUserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "admin_user_id is required")
	}
	w, err := s.e.Review(ctx, r.WithdrawalId, r.Approve, r.AdminUserId, r.Reason)
	if err != nil {
		return nil, mapErr(err)
	}
	slog.InfoContext(ctx, "withdrawal reviewed", "withdrawal", w.ID, "approve", r.Approve, "admin", r.AdminUserId)
	return &withdrawalsv1.ReviewWithdrawalResponse{Withdrawal: toProto(w)}, nil
}
