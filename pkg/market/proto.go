package market

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	"github.com/tickless/pkg/money"
)

// FromProto converts the wire representation of a market.
func FromProto(m *ordersv1.Market) (Spec, error) {
	minNotional, err := money.ParseInt128(m.GetMinNotional())
	if err != nil {
		return Spec{}, fmt.Errorf("market %s: min_notional: %w", m.GetSymbol(), err)
	}
	return Spec{
		Symbol: m.Symbol, Base: m.BaseAsset, Quote: m.QuoteAsset,
		BaseDecimals: uint8(m.BaseDecimals), QuoteDecimals: uint8(m.QuoteDecimals),
		TickSize: m.TickSize, LotSize: m.LotSize, MinNotional: minNotional,
		MakerFeeBps: m.MakerFeeBps, TakerFeeBps: m.TakerFeeBps,
		MinPriceTicks: m.MinPriceTicks, MaxPriceTicks: m.MaxPriceTicks,
		Halted: m.Status == ordersv1.MarketStatus_MARKET_STATUS_HALTED,
	}, nil
}

// ToProto converts a market for the wire.
func (s Spec) ToProto() *ordersv1.Market {
	st := ordersv1.MarketStatus_MARKET_STATUS_ACTIVE
	if s.Halted {
		st = ordersv1.MarketStatus_MARKET_STATUS_HALTED
	}
	return &ordersv1.Market{
		Symbol: s.Symbol, BaseAsset: s.Base, QuoteAsset: s.Quote,
		BaseDecimals: uint32(s.BaseDecimals), QuoteDecimals: uint32(s.QuoteDecimals),
		TickSize: s.TickSize, LotSize: s.LotSize, MinNotional: s.MinNotional.String(),
		MakerFeeBps: s.MakerFeeBps, TakerFeeBps: s.TakerFeeBps,
		MinPriceTicks: s.MinPriceTicks, MaxPriceTicks: s.MaxPriceTicks, Status: st,
	}
}

// Cache holds a periodically refreshed copy of the market configuration
// owned by the orders service. Services that only need to READ market config
// (ledger, gateway, marketdata) use it instead of sharing a database.
type Cache struct {
	fetch func(context.Context) ([]Spec, error)
	mu    sync.RWMutex
	m     map[string]Spec
	ready chan struct{}
	once  sync.Once
}

// NewCache creates a cache fed by fetch.
func NewCache(fetch func(context.Context) ([]Spec, error)) *Cache {
	return &Cache{fetch: fetch, m: map[string]Spec{}, ready: make(chan struct{})}
}

// Refresh loads the markets once.
func (c *Cache) Refresh(ctx context.Context) error {
	specs, err := c.fetch(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]Spec, len(specs))
	for _, s := range specs {
		m[s.Symbol] = s
	}
	c.mu.Lock()
	c.m = m
	c.mu.Unlock()
	c.once.Do(func() { close(c.ready) })
	return nil
}

// Run refreshes every interval until ctx is cancelled, retrying quickly until
// the first successful load (the owning service may start later).
func (c *Cache) Run(ctx context.Context, interval time.Duration) {
	for ctx.Err() == nil {
		wait := interval
		if err := c.Refresh(ctx); err != nil {
			slog.Warn("market refresh failed", "err", err)
			wait = time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
	}
}

// Ready is closed after the first successful refresh.
func (c *Cache) Ready() <-chan struct{} { return c.ready }

// Spec returns a market by symbol.
func (c *Cache) Spec(symbol string) (Spec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.m[symbol]
	return s, ok
}

// All returns all markets sorted by symbol.
func (c *Cache) All() []Spec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Spec, 0, len(c.m))
	for _, s := range c.m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

// FetchFromOrders returns a fetch function backed by the orders gRPC API.
func FetchFromOrders(cl ordersv1.OrdersServiceClient) func(context.Context) ([]Spec, error) {
	return func(ctx context.Context) ([]Spec, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, err := cl.ListMarkets(ctx, &ordersv1.ListMarketsRequest{})
		if err != nil {
			return nil, err
		}
		out := make([]Spec, 0, len(resp.Markets))
		for _, m := range resp.Markets {
			s, err := FromProto(m)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return out, nil
	}
}
