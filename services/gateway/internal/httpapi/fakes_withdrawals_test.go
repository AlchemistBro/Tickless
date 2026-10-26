package httpapi_test

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
)

func (f *fakeIdentity) GetUser(_ context.Context, r *identityv1.GetUserRequest, _ ...grpc.CallOption) (*identityv1.GetUserResponse, error) {
	return &identityv1.GetUserResponse{UserId: r.UserId, Email: "u@example.com", Role: "user", EmailVerified: true, TotpEnabled: true}, nil
}

func (f *fakeIdentity) EnrollTOTP(context.Context, *identityv1.EnrollTOTPRequest, ...grpc.CallOption) (*identityv1.EnrollTOTPResponse, error) {
	return &identityv1.EnrollTOTPResponse{Secret: "JBSWY3DPEHPK3PXP", OtpauthUri: "otpauth://totp/Tickless:u?secret=JBSWY3DPEHPK3PXP"}, nil
}

func (f *fakeIdentity) ConfirmTOTP(_ context.Context, r *identityv1.ConfirmTOTPRequest, _ ...grpc.CallOption) (*identityv1.ConfirmTOTPResponse, error) {
	if r.Code != "123456" {
		return nil, status.Error(codes.Unauthenticated, "invalid two-factor code")
	}
	return &identityv1.ConfirmTOTPResponse{}, nil
}

func (f *fakeIdentity) SetNotificationPrefs(context.Context, *identityv1.SetNotificationPrefsRequest, ...grpc.CallOption) (*identityv1.SetNotificationPrefsResponse, error) {
	return &identityv1.SetNotificationPrefsResponse{}, nil
}

// fakeWithdrawals records requests and serves one withdrawal per request.
type fakeWithdrawals struct {
	withdrawalsv1.WithdrawalsServiceClient
	mu       sync.Mutex
	requests []*withdrawalsv1.RequestWithdrawalRequest
	reviews  []*withdrawalsv1.ReviewWithdrawalRequest
}

func sampleWithdrawal(id, user int64, state string) *withdrawalsv1.Withdrawal {
	return &withdrawalsv1.Withdrawal{Id: id, UserId: user, Chain: "btc", Asset: "BTC", Amount: &commonv1.Amount{Units: "5000000"},
		Destination: "bcrt1qdest", State: state, CreatedAtUnixNano: 1_700_000_000_000_000_000, UpdatedAtUnixNano: 1_700_000_001_000_000_000}
}

func (f *fakeWithdrawals) RequestWithdrawal(_ context.Context, r *withdrawalsv1.RequestWithdrawalRequest, _ ...grpc.CallOption) (*withdrawalsv1.RequestWithdrawalResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, r)
	f.mu.Unlock()
	if r.TotpCode != "123456" {
		return nil, status.Error(codes.Unauthenticated, "invalid two-factor code")
	}
	w := sampleWithdrawal(1, r.UserId, "REQUESTED")
	w.Amount = r.Amount
	return &withdrawalsv1.RequestWithdrawalResponse{Withdrawal: w}, nil
}

func (f *fakeWithdrawals) ListWithdrawals(_ context.Context, r *withdrawalsv1.ListWithdrawalsRequest, _ ...grpc.CallOption) (*withdrawalsv1.ListWithdrawalsResponse, error) {
	return &withdrawalsv1.ListWithdrawalsResponse{Withdrawals: []*withdrawalsv1.Withdrawal{sampleWithdrawal(1, r.UserId, "BROADCAST")}}, nil
}

func (f *fakeWithdrawals) GetWithdrawal(_ context.Context, r *withdrawalsv1.GetWithdrawalRequest, _ ...grpc.CallOption) (*withdrawalsv1.GetWithdrawalResponse, error) {
	if r.WithdrawalId != 1 {
		return nil, status.Error(codes.NotFound, "withdrawal not found")
	}
	return &withdrawalsv1.GetWithdrawalResponse{Withdrawal: sampleWithdrawal(1, r.UserId, "CONFIRMED")}, nil
}

func (f *fakeWithdrawals) ListPendingReview(context.Context, *withdrawalsv1.ListPendingReviewRequest, ...grpc.CallOption) (*withdrawalsv1.ListPendingReviewResponse, error) {
	return &withdrawalsv1.ListPendingReviewResponse{Withdrawals: []*withdrawalsv1.Withdrawal{sampleWithdrawal(7, 2, "PENDING_REVIEW")}}, nil
}

func (f *fakeWithdrawals) ReviewWithdrawal(_ context.Context, r *withdrawalsv1.ReviewWithdrawalRequest, _ ...grpc.CallOption) (*withdrawalsv1.ReviewWithdrawalResponse, error) {
	f.mu.Lock()
	f.reviews = append(f.reviews, r)
	f.mu.Unlock()
	state := "APPROVED"
	if !r.Approve {
		state = "REJECTED"
	}
	return &withdrawalsv1.ReviewWithdrawalResponse{Withdrawal: sampleWithdrawal(r.WithdrawalId, 2, state)}, nil
}
