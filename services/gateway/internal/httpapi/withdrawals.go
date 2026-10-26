package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/money"
)

// HeaderTOTP carries the second-factor code of a withdrawal request.
const HeaderTOTP = "X-TOTP"

// ---------------------------------------------------------------- profile & 2FA

type meBody struct {
	UserID           string `json:"user_id"`
	Email            string `json:"email"`
	Role             string `json:"role"`
	EmailVerified    bool   `json:"email_verified"`
	TOTPEnabled      bool   `json:"totp_enabled"`
	NotifyOrderFills bool   `json:"notify_order_fills"`
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	u, err := s.d.Identity.GetUser(r.Context(), &identityv1.GetUserRequest{UserId: p.UserID})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, meBody{UserID: itoa64(u.UserId), Email: u.Email, Role: u.Role, EmailVerified: u.EmailVerified,
		TOTPEnabled: u.TotpEnabled, NotifyOrderFills: u.NotifyOrderFills})
}

func (s *Server) setNotifications(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	var in struct {
		OrderFills *bool `json:"order_fills"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.OrderFills == nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_argument", "order_fills is required")
		return
	}
	if _, err := s.d.Identity.SetNotificationPrefs(r.Context(), &identityv1.SetNotificationPrefsRequest{UserId: p.UserID, NotifyOrderFills: *in.OrderFills}); err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) enrollTOTP(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	resp, err := s.d.Identity.EnrollTOTP(r.Context(), &identityv1.EnrollTOTPRequest{UserId: p.UserID})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"secret": resp.Secret, "otpauth_uri": resp.OtpauthUri})
}

func (s *Server) confirmTOTP(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	var in struct {
		Code string `json:"code"`
	}
	if !decode(w, r, &in) {
		return
	}
	if _, err := s.d.Identity.ConfirmTOTP(r.Context(), &identityv1.ConfirmTOTPRequest{UserId: p.UserID, Code: strings.TrimSpace(in.Code)}); err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------- withdrawals

type withdrawalBody struct {
	ID            string `json:"id"`
	Chain         string `json:"chain"`
	Asset         string `json:"asset"`
	Amount        string `json:"amount"`
	Destination   string `json:"destination"`
	State         string `json:"state"`
	Reason        string `json:"reason,omitempty"`
	TxID          string `json:"txid,omitempty"`
	Confirmations int64  `json:"confirmations"`
	UserID        string `json:"user_id,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

func withdrawalJSON(x *withdrawalsv1.Withdrawal, withUser bool) withdrawalBody {
	dec, known := assetDecimals(x.Asset)
	b := withdrawalBody{ID: itoa64(x.Id), Chain: x.Chain, Asset: x.Asset, Amount: formatUnits(x.GetAmount().GetUnits(), dec, known),
		Destination: x.Destination, State: strings.ToLower(x.State), Reason: x.Reason, TxID: x.Txid, Confirmations: x.Confirmations,
		CreatedAt: time.Unix(0, x.CreatedAtUnixNano).UTC().Format(time.RFC3339Nano),
		UpdatedAt: time.Unix(0, x.UpdatedAtUnixNano).UTC().Format(time.RFC3339Nano)}
	if withUser {
		b.UserID = itoa64(x.UserId)
	}
	return b
}

func (s *Server) withdrawalsReady(w http.ResponseWriter) bool {
	if s.d.Withdrawals == nil {
		writeErr(w, http.StatusServiceUnavailable, "withdrawals_unavailable", "withdrawals are not enabled")
		return false
	}
	return true
}

func (s *Server) requestWithdrawal(w http.ResponseWriter, r *http.Request) {
	if !s.withdrawalsReady(w) {
		return
	}
	p, _ := principalFrom(r.Context())
	var in struct {
		Chain       string `json:"chain"`
		Asset       string `json:"asset"`
		Amount      string `json:"amount"`
		Destination string `json:"destination"`
	}
	if !decode(w, r, &in) {
		return
	}
	code := strings.TrimSpace(r.Header.Get(HeaderTOTP))
	if code == "" {
		writeErr(w, http.StatusUnauthorized, "totp_required", "send the authenticator code in the X-TOTP header")
		return
	}
	asset, ok := money.LookupAsset(strings.ToUpper(in.Asset))
	if !ok {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_asset", "unknown asset")
		return
	}
	units, err := asset.Parse(strings.TrimSpace(in.Amount))
	if err != nil || units.Sign() <= 0 {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_amount", "amount must be a positive decimal within the asset's precision")
		return
	}
	resp, err := s.d.Withdrawals.RequestWithdrawal(r.Context(), &withdrawalsv1.RequestWithdrawalRequest{UserId: p.UserID,
		Chain: strings.ToLower(in.Chain), Asset: asset.Symbol, Amount: &commonv1.Amount{Units: units.String()},
		Destination: strings.TrimSpace(in.Destination), TotpCode: code})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, withdrawalJSON(resp.Withdrawal, false))
}

func (s *Server) listWithdrawals(w http.ResponseWriter, r *http.Request) {
	if !s.withdrawalsReady(w) {
		return
	}
	p, _ := principalFrom(r.Context())
	resp, err := s.d.Withdrawals.ListWithdrawals(r.Context(), &withdrawalsv1.ListWithdrawalsRequest{UserId: p.UserID})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]withdrawalBody, 0, len(resp.Withdrawals))
	for _, x := range resp.Withdrawals {
		out = append(out, withdrawalJSON(x, false))
	}
	writeJSON(w, http.StatusOK, map[string]any{"withdrawals": out})
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusNotFound, "not_found", "not found")
		return 0, false
	}
	return id, true
}

func (s *Server) getWithdrawal(w http.ResponseWriter, r *http.Request) {
	if !s.withdrawalsReady(w) {
		return
	}
	p, _ := principalFrom(r.Context())
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	resp, err := s.d.Withdrawals.GetWithdrawal(r.Context(), &withdrawalsv1.GetWithdrawalRequest{UserId: p.UserID, WithdrawalId: id})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, withdrawalJSON(resp.Withdrawal, false))
}

func (s *Server) adminReviewQueue(w http.ResponseWriter, r *http.Request) {
	if !s.withdrawalsReady(w) {
		return
	}
	resp, err := s.d.Withdrawals.ListPendingReview(r.Context(), &withdrawalsv1.ListPendingReviewRequest{})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]withdrawalBody, 0, len(resp.Withdrawals))
	for _, x := range resp.Withdrawals {
		out = append(out, withdrawalJSON(x, true))
	}
	writeJSON(w, http.StatusOK, map[string]any{"withdrawals": out})
}

func (s *Server) adminReview(approve bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.withdrawalsReady(w) {
			return
		}
		p, _ := principalFrom(r.Context())
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var in struct {
			Reason string `json:"reason"`
		}
		if r.ContentLength > 0 && !decode(w, r, &in) {
			return
		}
		resp, err := s.d.Withdrawals.ReviewWithdrawal(r.Context(), &withdrawalsv1.ReviewWithdrawalRequest{WithdrawalId: id, Approve: approve,
			AdminUserId: p.UserID, Reason: in.Reason})
		if err != nil {
			writeGRPCErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, withdrawalJSON(resp.Withdrawal, true))
	}
}
