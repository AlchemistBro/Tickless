package httpapi_test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/gateway/internal/httpapi"
	"github.com/tickless/pkg/auth"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	walletv1 "github.com/tickless/pkg/gen/tickless/wallet/v1"
)

// fakeWallet derives a fake address per (user, chain) and records callers.
type fakeWallet struct {
	walletv1.WalletServiceClient
	mu    sync.Mutex
	users []int64
}

func (f *fakeWallet) GetDepositAddress(_ context.Context, r *walletv1.GetDepositAddressRequest, _ ...grpc.CallOption) (*walletv1.GetDepositAddressResponse, error) {
	f.mu.Lock()
	f.users = append(f.users, r.UserId)
	f.mu.Unlock()
	return &walletv1.GetDepositAddressResponse{Chain: r.Chain, Address: fmt.Sprintf("%s-addr-of-%d", r.Chain, r.UserId), DerivationIndex: uint32(r.UserId)}, nil //nolint:gosec // test ids are small
}

// fakeChainwatcher returns one pending BTC and one confirmed ETH deposit of the caller.
type fakeChainwatcher struct {
	chainwatcherv1.ChainwatcherServiceClient
	mu    sync.Mutex
	users []int64
}

func (f *fakeChainwatcher) ListDeposits(_ context.Context, r *chainwatcherv1.ListDepositsRequest, _ ...grpc.CallOption) (*chainwatcherv1.ListDepositsResponse, error) {
	f.mu.Lock()
	f.users = append(f.users, r.UserId)
	f.mu.Unlock()
	return &chainwatcherv1.ListDepositsResponse{Deposits: []*chainwatcherv1.Deposit{
		{Id: 2, Chain: "btc", Txid: "ab", Index: 1, Asset: "BTC", Amount: &commonv1.Amount{Units: "50000000"}, Address: "bcrt1q",
			Status: chainwatcherv1.DepositStatus_DEPOSIT_STATUS_PENDING, BlockHeight: 120, Confirmations: 1, RequiredConfirmations: 2, DetectedAtUnixNano: 1_700_000_000_000_000_000},
		{Id: 1, Chain: "eth", Txid: "0xcd", Index: -1, Asset: "ETH", Amount: &commonv1.Amount{Units: "1500000000000000000"}, Address: "0xAb",
			Status: chainwatcherv1.DepositStatus_DEPOSIT_STATUS_CONFIRMED, BlockHeight: 40, Confirmations: 12, RequiredConfirmations: 12,
			DetectedAtUnixNano: 1_690_000_000_000_000_000, ConfirmedAtUnixNano: 1_690_000_100_000_000_000},
	}}, nil
}

// fakeIdentity implements only what the gateway calls; any other method panics
// (nil embedded interface), which makes accidental calls loud.
type fakeIdentity struct {
	identityv1.IdentityServiceClient
	mu             sync.Mutex
	keys           map[string]*identityv1.LookupAPIKeyResponse
	lookups        atomic.Int32
	loginErr       error
	frozenSet      []int64
	frozenOnServer []int64
	listFrozenErr  error
	pub            func() ed25519.PublicKey
}

func (f *fakeIdentity) Signup(_ context.Context, r *identityv1.SignupRequest, _ ...grpc.CallOption) (*identityv1.SignupResponse, error) {
	if r.Email == "taken@example.com" {
		return nil, status.Error(codes.AlreadyExists, "identity: email already registered")
	}
	return &identityv1.SignupResponse{UserId: 7}, nil
}

func (f *fakeIdentity) Login(_ context.Context, r *identityv1.LoginRequest, _ ...grpc.CallOption) (*identityv1.LoginResponse, error) {
	if f.loginErr != nil {
		return nil, f.loginErr
	}
	return &identityv1.LoginResponse{Tokens: &identityv1.TokenPair{AccessToken: "a", AccessExpiresUnix: 1_800_000_000, RefreshToken: "r", RefreshExpiresUnix: 1_900_000_000}}, nil
}

func (f *fakeIdentity) LookupAPIKey(_ context.Context, r *identityv1.LookupAPIKeyRequest, _ ...grpc.CallOption) (*identityv1.LookupAPIKeyResponse, error) {
	f.lookups.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if k, ok := f.keys[r.KeyId]; ok {
		return k, nil
	}
	return nil, status.Error(codes.NotFound, "identity: not found")
}

func (f *fakeIdentity) CreateAPIKey(_ context.Context, r *identityv1.CreateAPIKeyRequest, _ ...grpc.CallOption) (*identityv1.CreateAPIKeyResponse, error) {
	return &identityv1.CreateAPIKeyResponse{Key: &identityv1.APIKeyInfo{KeyId: "tk_1", Label: r.Label, CanTrade: r.CanTrade, CreatedAtUnix: 1_700_000_000}, Secret: "s3cret"}, nil
}

func (f *fakeIdentity) ListUsers(context.Context, *identityv1.ListUsersRequest, ...grpc.CallOption) (*identityv1.ListUsersResponse, error) {
	return &identityv1.ListUsersResponse{Users: []*identityv1.UserInfo{{UserId: 1, Email: "a@b.co", Role: "admin", EmailVerified: true, CreatedAtUnix: 1_700_000_000}}}, nil
}

func (f *fakeIdentity) SetUserFrozen(_ context.Context, r *identityv1.SetUserFrozenRequest, _ ...grpc.CallOption) (*identityv1.SetUserFrozenResponse, error) {
	f.mu.Lock()
	f.frozenSet = append(f.frozenSet, r.UserId)
	f.mu.Unlock()
	return &identityv1.SetUserFrozenResponse{}, nil
}

func (f *fakeIdentity) ListFrozen(context.Context, *identityv1.ListFrozenRequest, ...grpc.CallOption) (*identityv1.ListFrozenResponse, error) {
	if f.listFrozenErr != nil {
		return nil, f.listFrozenErr
	}
	return &identityv1.ListFrozenResponse{UserIds: f.frozenOnServer}, nil
}

func (f *fakeIdentity) GetPublicKey(context.Context, *identityv1.GetPublicKeyRequest, ...grpc.CallOption) (*identityv1.GetPublicKeyResponse, error) {
	return &identityv1.GetPublicKeyResponse{Ed25519PublicKey: f.pub()}, nil
}

type fakeLedger struct {
	ledgerv1.LedgerServiceClient
}

func (fakeLedger) GetBalances(_ context.Context, r *ledgerv1.GetBalancesRequest, _ ...grpc.CallOption) (*ledgerv1.GetBalancesResponse, error) {
	return &ledgerv1.GetBalancesResponse{Balances: []*ledgerv1.Balance{{
		Asset: "USDT", Available: &commonv1.Amount{Units: "5500000000"}, Held: &commonv1.Amount{Units: "0"}}}}, nil
}

func (fakeLedger) ListEntries(context.Context, *ledgerv1.ListEntriesRequest, ...grpc.CallOption) (*ledgerv1.ListEntriesResponse, error) {
	return &ledgerv1.ListEntriesResponse{Entries: []*ledgerv1.Entry{{Id: 3, BusinessKey: "hold:1", Kind: "hold", CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Postings: []*ledgerv1.Posting{{Asset: "USDT", AccountKind: "AVAILABLE", Amount: &commonv1.Amount{Units: "-25000000000"}}}}}}, nil
}

type fakeOrders struct {
	ordersv1.OrdersServiceClient
	placed atomic.Int32
	err    error
	last   *ordersv1.PlaceOrderRequest
	mu     sync.Mutex
	seen   map[string]uint64
}

func sampleOrder(id uint64, market string) *ordersv1.Order {
	return &ordersv1.Order{Id: id, UserId: 1, Market: market, Side: commonv1.Side_SIDE_BUY, OrderType: "limit", TimeInForce: "GTC",
		Price: "50000", Quantity: "0.5", FilledQuantity: "0", Status: ordersv1.OrderStatus_ORDER_STATUS_NEW,
		CreatedAtUnixNano: 1_700_000_000_000_000_000, UpdatedAtUnixNano: 1_700_000_000_000_000_000}
}

func (f *fakeOrders) PlaceOrder(_ context.Context, r *ordersv1.PlaceOrderRequest, _ ...grpc.CallOption) (*ordersv1.PlaceOrderResponse, error) {
	f.mu.Lock()
	f.last = r
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen == nil {
		f.seen = map[string]uint64{}
	}
	key := fmt.Sprint(r.UserId, "/", r.ClientOrderId)
	if id, ok := f.seen[key]; ok && r.ClientOrderId != "" {
		// idempotent by client_order_id, like the real orders service
		return &ordersv1.PlaceOrderResponse{Order: sampleOrder(id, r.Market), Duplicate: true}, nil
	}
	n := uint64(f.placed.Add(1))
	if r.ClientOrderId != "" {
		f.seen[key] = n
	}
	return &ordersv1.PlaceOrderResponse{Order: sampleOrder(n, r.Market), Duplicate: false}, nil
}

func (f *fakeOrders) CancelOrder(_ context.Context, r *ordersv1.CancelOrderRequest, _ ...grpc.CallOption) (*ordersv1.CancelOrderResponse, error) {
	return &ordersv1.CancelOrderResponse{Order: sampleOrder(r.OrderId, "BTC-USDT")}, nil
}

func (f *fakeOrders) CancelAll(context.Context, *ordersv1.CancelAllRequest, ...grpc.CallOption) (*ordersv1.CancelAllResponse, error) {
	return &ordersv1.CancelAllResponse{Requested: 2}, nil
}

func (f *fakeOrders) GetOrder(_ context.Context, r *ordersv1.GetOrderRequest, _ ...grpc.CallOption) (*ordersv1.GetOrderResponse, error) {
	if r.OrderId == 404 {
		return nil, status.Error(codes.NotFound, "orders: order not found")
	}
	return &ordersv1.GetOrderResponse{Order: sampleOrder(r.OrderId, "BTC-USDT")}, nil
}

func (f *fakeOrders) ListOrders(context.Context, *ordersv1.ListOrdersRequest, ...grpc.CallOption) (*ordersv1.ListOrdersResponse, error) {
	return &ordersv1.ListOrdersResponse{Orders: []*ordersv1.Order{sampleOrder(2, "BTC-USDT"), sampleOrder(1, "BTC-USDT")}}, nil
}

var btcMarket = &ordersv1.Market{Symbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", BaseDecimals: 8, QuoteDecimals: 6,
	TickSize: 10000, LotSize: 1000, MinNotional: "5000000", MakerFeeBps: 10, TakerFeeBps: 20, MinPriceTicks: 1, MaxPriceTicks: 1_000_000_000,
	Status: ordersv1.MarketStatus_MARKET_STATUS_ACTIVE}

func (f *fakeOrders) ListMarkets(context.Context, *ordersv1.ListMarketsRequest, ...grpc.CallOption) (*ordersv1.ListMarketsResponse, error) {
	return &ordersv1.ListMarketsResponse{Markets: []*ordersv1.Market{btcMarket}}, nil
}

func (f *fakeOrders) SetMarketStatus(_ context.Context, r *ordersv1.SetMarketStatusRequest, _ ...grpc.CallOption) (*ordersv1.SetMarketStatusResponse, error) {
	if r.Market != "BTC-USDT" {
		return nil, status.Error(codes.NotFound, "orders: unknown market")
	}
	m := proto.Clone(btcMarket).(*ordersv1.Market)
	m.Status = r.Status
	return &ordersv1.SetMarketStatusResponse{Market: m}, nil
}

type testEnv struct {
	srv      *httpapi.Server
	identity *fakeIdentity
	orders   *fakeOrders
	wallet   *fakeWallet
	cw       *fakeChainwatcher
	redis    *miniredis.Miniredis
	rdb      *redis.Client
	signer   *auth.Signer
	now      *atomic.Int64
}

func newEnv(t *testing.T, limits httpapi.Limits) *testEnv { return newEnvSync(t, limits, false) }

func newEnvSync(t *testing.T, limits httpapi.Limits, requireFrozenSync bool) *testEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(nil, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	e := &testEnv{redis: mr, rdb: rdb, signer: signer, now: &atomic.Int64{}}
	e.now.Store(time.Now().UnixNano())
	e.identity = &fakeIdentity{keys: map[string]*identityv1.LookupAPIKeyResponse{}, pub: func() ed25519.PublicKey { return e.signer.PublicKey() }}
	e.orders = &fakeOrders{}
	e.wallet, e.cw = &fakeWallet{}, &fakeChainwatcher{}
	e.srv = httpapi.New(httpapi.Deps{
		Identity: e.identity, Ledger: fakeLedger{}, Orders: e.orders, Wallet: e.wallet, Chainwatcher: e.cw, Redis: rdb, Limits: limits,
		AllowedOrigins: []string{"http://localhost:5173"}, RequireFrozenSync: requireFrozenSync, MarketData: "http://127.0.0.1:1",
		PublicKey: func(context.Context) (ed25519.PublicKey, error) { return e.identity.pub(), nil },
		Now:       func() time.Time { return time.Unix(0, e.now.Load()) },
	})
	return e
}

func (e *testEnv) token(uid int64, role string) string { return e.tokenV(uid, role, true) }

func (e *testEnv) tokenV(uid int64, role string, verified bool) string {
	tok, _, err := e.signer.Issue(auth.Claims{UserID: uid, Role: role, EmailVerified: verified}, time.Unix(0, e.now.Load()))
	if err != nil {
		panic(err)
	}
	return tok
}

var generousLimits = httpapi.Limits{IPRate: 1e6, IPBurst: 1e6, AuthRate: 1e6, AuthBurst: 1e6, UserRate: 1e6, UserBurst: 1e6, OrderRate: 1e6, OrderBurst: 1e6}
