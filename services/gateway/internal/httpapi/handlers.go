package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
)

// ---- auth ----

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type tokenBody struct {
	AccessToken      string `json:"access_token"`
	AccessExpiresAt  string `json:"access_expires_at"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresAt string `json:"refresh_expires_at"`
}

func tokens(t *identityv1.TokenPair) tokenBody {
	return tokenBody{
		AccessToken: t.AccessToken, AccessExpiresAt: time.Unix(t.AccessExpiresUnix, 0).UTC().Format(time.RFC3339),
		RefreshToken: t.RefreshToken, RefreshExpiresAt: time.Unix(t.RefreshExpiresUnix, 0).UTC().Format(time.RFC3339),
	}
}

func (s *Server) signup(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !decode(w, r, &in) {
		return
	}
	resp, err := s.d.Identity.Signup(r.Context(), &identityv1.SignupRequest{Email: in.Email, Password: in.Password})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"user_id": itoa64(resp.UserId)})
}

func (s *Server) verifyEmail(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Token string `json:"token"`
	}
	if !decode(w, r, &in) {
		return
	}
	if _, err := s.d.Identity.VerifyEmail(r.Context(), &identityv1.VerifyEmailRequest{Token: in.Token}); err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !decode(w, r, &in) {
		return
	}
	resp, err := s.d.Identity.Login(r.Context(), &identityv1.LoginRequest{Email: in.Email, Password: in.Password})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tokens(resp.Tokens))
}

type refreshIn struct {
	RefreshToken string `json:"refresh_token"`
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var in refreshIn
	if !decode(w, r, &in) {
		return
	}
	resp, err := s.d.Identity.Refresh(r.Context(), &identityv1.RefreshRequest{RefreshToken: in.RefreshToken})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tokens(resp.Tokens))
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	var in refreshIn
	if !decode(w, r, &in) {
		return
	}
	if _, err := s.d.Identity.Logout(r.Context(), &identityv1.LogoutRequest{RefreshToken: in.RefreshToken}); err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- markets ----

type marketBody struct {
	Symbol      string `json:"symbol"`
	Base        string `json:"base_asset"`
	Quote       string `json:"quote_asset"`
	TickSize    string `json:"tick_size"`
	LotSize     string `json:"lot_size"`
	MinNotional string `json:"min_notional"`
	MakerFeeBps uint32 `json:"maker_fee_bps"`
	TakerFeeBps uint32 `json:"taker_fee_bps"`
	Status      string `json:"status"`
}

func marketJSON(m *ordersv1.Market) (marketBody, error) {
	spec, err := market.FromProto(m)
	if err != nil {
		return marketBody{}, err
	}
	// tick and lot sizes are shown as decimals in whole units (price step, quantity step).
	tick := spec.FormatPrice(1)
	lot := spec.FormatQty(1)
	st := "active"
	if spec.Halted {
		st = "halted"
	}
	return marketBody{
		Symbol: spec.Symbol, Base: spec.Base, Quote: spec.Quote, TickSize: tick, LotSize: lot,
		MinNotional: money.Asset{Decimals: spec.QuoteDecimals}.Format(spec.MinNotional), MakerFeeBps: spec.MakerFeeBps, TakerFeeBps: spec.TakerFeeBps, Status: st,
	}, nil
}

func (s *Server) listMarkets(w http.ResponseWriter, r *http.Request) {
	resp, err := s.d.Orders.ListMarkets(r.Context(), &ordersv1.ListMarketsRequest{})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]marketBody, 0, len(resp.Markets))
	for _, m := range resp.Markets {
		b, err := marketJSON(m)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "invalid market configuration")
			return
		}
		out = append(out, b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"markets": out})
}

// ---- balances / ledger ----

type balanceBody struct {
	Asset     string `json:"asset"`
	Available string `json:"available"`
	Held      string `json:"held"`
}

func (s *Server) balancesFor(w http.ResponseWriter, r *http.Request, uid int64) {
	resp, err := s.d.Ledger.GetBalances(r.Context(), &ledgerv1.GetBalancesRequest{UserId: uid})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]balanceBody, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		d, known := assetDecimals(b.Asset)
		out = append(out, balanceBody{Asset: b.Asset, Available: formatUnits(b.Available.GetUnits(), d, known), Held: formatUnits(b.Held.GetUnits(), d, known)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"balances": out})
}

func (s *Server) balances(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	s.balancesFor(w, r, p.UserID)
}

type postingBody struct {
	Asset  string `json:"asset"`
	Kind   string `json:"account"`
	Amount string `json:"amount"`
}

type entryBody struct {
	ID        string        `json:"id"`
	Reference string        `json:"reference"`
	Kind      string        `json:"kind"`
	CreatedAt string        `json:"created_at"`
	Postings  []postingBody `json:"postings"`
}

func (s *Server) ledgerFor(w http.ResponseWriter, r *http.Request, uid int64) {
	before, _ := strconv.ParseInt(r.URL.Query().Get("before_id"), 10, 64)
	limit := queryInt32(r, "limit")
	resp, err := s.d.Ledger.ListEntries(r.Context(), &ledgerv1.ListEntriesRequest{
		UserId: uid, Asset: strings.ToUpper(r.URL.Query().Get("asset")), BeforeId: before, Limit: limit})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]entryBody, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		eb := entryBody{ID: itoa64(e.Id), Reference: e.BusinessKey, Kind: e.Kind,
			CreatedAt: time.Unix(0, e.CreatedAtUnixNano).UTC().Format(time.RFC3339Nano)}
		for _, p := range e.Postings {
			d, known := assetDecimals(p.Asset)
			eb.Postings = append(eb.Postings, postingBody{Asset: p.Asset, Kind: strings.ToLower(p.AccountKind),
				Amount: formatUnits(p.Amount.GetUnits(), d, known)})
		}
		out = append(out, eb)
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out})
}

func (s *Server) ledgerEntries(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	s.ledgerFor(w, r, p.UserID)
}

// ---- orders ----

type orderIn struct {
	Market        string `json:"market"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	TimeInForce   string `json:"time_in_force"`
	PostOnly      bool   `json:"post_only"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	ClientOrderID string `json:"client_order_id"`
}

type orderBody struct {
	ID             string `json:"id"`
	Market         string `json:"market"`
	Side           string `json:"side"`
	Type           string `json:"type"`
	TimeInForce    string `json:"time_in_force"`
	PostOnly       bool   `json:"post_only"`
	Price          string `json:"price,omitempty"`
	Quantity       string `json:"quantity"`
	FilledQuantity string `json:"filled_quantity"`
	Status         string `json:"status"`
	ClientOrderID  string `json:"client_order_id,omitempty"`
	RejectReason   string `json:"reject_reason,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

func orderJSON(o *ordersv1.Order) orderBody {
	side := "buy"
	if o.Side == commonv1.Side_SIDE_SELL {
		side = "sell"
	}
	st := strings.ToLower(strings.TrimPrefix(o.Status.String(), "ORDER_STATUS_"))
	return orderBody{
		ID: strconv.FormatUint(o.Id, 10), Market: o.Market, Side: side, Type: o.OrderType, TimeInForce: o.TimeInForce,
		PostOnly: o.PostOnly, Price: o.Price, Quantity: o.Quantity, FilledQuantity: o.FilledQuantity, Status: st,
		ClientOrderID: o.ClientOrderId, RejectReason: o.RejectReason,
		CreatedAt: time.Unix(0, o.CreatedAtUnixNano).UTC().Format(time.RFC3339Nano),
		UpdatedAt: time.Unix(0, o.UpdatedAtUnixNano).UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) placeOrder(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	var in orderIn
	if !decode(w, r, &in) {
		return
	}
	var side commonv1.Side
	switch strings.ToLower(in.Side) {
	case "buy":
		side = commonv1.Side_SIDE_BUY
	case "sell":
		side = commonv1.Side_SIDE_SELL
	default:
		writeErr(w, http.StatusBadRequest, "invalid_argument", `side must be "buy" or "sell"`)
		return
	}
	resp, err := s.d.Orders.PlaceOrder(r.Context(), &ordersv1.PlaceOrderRequest{
		UserId: p.UserID, Market: in.Market, Side: side, OrderType: strings.ToLower(in.Type), TimeInForce: in.TimeInForce,
		PostOnly: in.PostOnly, Price: in.Price, Quantity: in.Quantity, ClientOrderId: in.ClientOrderID,
	})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	code := http.StatusCreated
	if resp.Duplicate {
		code = http.StatusOK
	}
	writeJSON(w, code, orderJSON(resp.Order))
}

func parseOrderID(w http.ResponseWriter, raw string) (uint64, bool) {
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "invalid order id")
		return 0, false
	}
	return id, true
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	id, ok := parseOrderID(w, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	resp, err := s.d.Orders.GetOrder(r.Context(), &ordersv1.GetOrderRequest{UserId: p.UserID, OrderId: id})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, orderJSON(resp.Order))
}

func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	id, ok := parseOrderID(w, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	resp, err := s.d.Orders.CancelOrder(r.Context(), &ordersv1.CancelOrderRequest{UserId: p.UserID, OrderId: id})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, orderJSON(resp.Order))
}

func (s *Server) cancelAll(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	resp, err := s.d.Orders.CancelAll(r.Context(), &ordersv1.CancelAllRequest{UserId: p.UserID, Market: r.URL.Query().Get("market")})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"cancel_requested": resp.Requested})
}

func (s *Server) listOrders(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	q := r.URL.Query()
	before, _ := strconv.ParseUint(q.Get("before_id"), 10, 64)
	limit := queryInt32(r, "limit")
	resp, err := s.d.Orders.ListOrders(r.Context(), &ordersv1.ListOrdersRequest{
		UserId: p.UserID, Market: q.Get("market"), OpenOnly: q.Get("open") == "true", BeforeId: before, Limit: limit})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]orderBody, 0, len(resp.Orders))
	for _, o := range resp.Orders {
		out = append(out, orderJSON(o))
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": out})
}

// ---- API keys ----

type apiKeyBody struct {
	KeyID     string `json:"key_id"`
	Label     string `json:"label"`
	CanTrade  bool   `json:"can_trade"`
	CreatedAt string `json:"created_at"`
	Revoked   bool   `json:"revoked"`
	Secret    string `json:"secret,omitempty"`
}

func apiKeyJSON(k *identityv1.APIKeyInfo) apiKeyBody {
	return apiKeyBody{KeyID: k.KeyId, Label: k.Label, CanTrade: k.CanTrade, Revoked: k.Revoked,
		CreatedAt: time.Unix(k.CreatedAtUnix, 0).UTC().Format(time.RFC3339)}
}

func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	var in struct {
		Label    string `json:"label"`
		CanTrade bool   `json:"can_trade"`
	}
	if !decode(w, r, &in) {
		return
	}
	resp, err := s.d.Identity.CreateAPIKey(r.Context(), &identityv1.CreateAPIKeyRequest{UserId: p.UserID, Label: in.Label, CanTrade: in.CanTrade})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	b := apiKeyJSON(resp.Key)
	b.Secret = resp.Secret
	writeJSON(w, http.StatusCreated, b)
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	resp, err := s.d.Identity.ListAPIKeys(r.Context(), &identityv1.ListAPIKeysRequest{UserId: p.UserID})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]apiKeyBody, 0, len(resp.Keys))
	for _, k := range resp.Keys {
		out = append(out, apiKeyJSON(k))
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context())
	id := chi.URLParam(r, "keyId")
	if _, err := s.d.Identity.RevokeAPIKey(r.Context(), &identityv1.RevokeAPIKeyRequest{UserId: p.UserID, KeyId: id}); err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	s.keyCache.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

// ---- admin ----

type userBody struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	Role          string `json:"role"`
	EmailVerified bool   `json:"email_verified"`
	Frozen        bool   `json:"frozen"`
	CreatedAt     string `json:"created_at"`
}

func (s *Server) adminListUsers(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after_id"), 10, 64)
	limit := queryInt32(r, "limit")
	resp, err := s.d.Identity.ListUsers(r.Context(), &identityv1.ListUsersRequest{AfterId: after, Limit: limit})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]userBody, 0, len(resp.Users))
	for _, u := range resp.Users {
		out = append(out, userBody{ID: itoa64(u.UserId), Email: u.Email, Role: u.Role, EmailVerified: u.EmailVerified,
			Frozen: u.Frozen, CreatedAt: time.Unix(u.CreatedAtUnix, 0).UTC().Format(time.RFC3339)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

func pathUserID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "invalid user id")
		return 0, false
	}
	return id, true
}

func (s *Server) adminFreeze(frozen bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathUserID(w, r)
		if !ok {
			return
		}
		if _, err := s.d.Identity.SetUserFrozen(r.Context(), &identityv1.SetUserFrozenRequest{UserId: id, Frozen: frozen}); err != nil {
			writeGRPCErr(w, r, err)
			return
		}
		// Take effect immediately on this node; other nodes learn through users.events.
		if frozen {
			s.d.Redis.SAdd(r.Context(), FrozenSet, itoa64(id))
		} else {
			s.d.Redis.SRem(r.Context(), FrozenSet, itoa64(id))
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) adminBalances(w http.ResponseWriter, r *http.Request) {
	if id, ok := pathUserID(w, r); ok {
		s.balancesFor(w, r, id)
	}
}

func (s *Server) adminLedger(w http.ResponseWriter, r *http.Request) {
	if id, ok := pathUserID(w, r); ok {
		s.ledgerFor(w, r, id)
	}
}

func (s *Server) adminHalt(halt bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st := ordersv1.MarketStatus_MARKET_STATUS_ACTIVE
		if halt {
			st = ordersv1.MarketStatus_MARKET_STATUS_HALTED
		}
		resp, err := s.d.Orders.SetMarketStatus(r.Context(), &ordersv1.SetMarketStatusRequest{Market: chi.URLParam(r, "symbol"), Status: st})
		if err != nil {
			writeGRPCErr(w, r, err)
			return
		}
		b, err := marketJSON(resp.Market)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "invalid market configuration")
			return
		}
		writeJSON(w, http.StatusOK, b)
	}
}

// queryInt32 parses an optional non-negative int32 query parameter (0 if absent or invalid).
func queryInt32(r *http.Request, key string) int32 {
	n, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 32)
	if err != nil || n < 0 {
		return 0
	}
	return int32(n)
}
