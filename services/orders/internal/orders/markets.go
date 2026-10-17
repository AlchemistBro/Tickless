package orders

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
)

// marketInfo is a market plus the order-service-only parameters.
type marketInfo struct {
	market.Spec
	BandBps   int64
	LastPrice int64 // ticks, 0 = no trade yet
}

// Markets caches the market table (owned by this service) so that placing an
// order does not need a database read. It is refreshed every second, so a
// halt takes effect on every instance within about a second.
type Markets struct {
	pool *pgxpool.Pool
	mu   sync.RWMutex
	m    map[string]marketInfo
}

// NewMarkets loads the markets once and returns the cache.
func NewMarkets(ctx context.Context, pool *pgxpool.Pool) (*Markets, error) {
	ms := &Markets{pool: pool, m: map[string]marketInfo{}}
	return ms, ms.Refresh(ctx)
}

const marketCols = `symbol, base_asset, quote_asset, base_decimals, quote_decimals, tick_size, lot_size, min_notional::text,
	maker_fee_bps, taker_fee_bps, min_price_ticks, max_price_ticks, band_bps, COALESCE(last_price_ticks, 0), status`

func scanMarket(row pgx.Row) (marketInfo, error) {
	var mi marketInfo
	var bd, qd, mk, tk, band int
	var minNotional, status string
	err := row.Scan(&mi.Symbol, &mi.Base, &mi.Quote, &bd, &qd, &mi.TickSize, &mi.LotSize, &minNotional,
		&mk, &tk, &mi.MinPriceTicks, &mi.MaxPriceTicks, &band, &mi.LastPrice, &status)
	if err != nil {
		return mi, err
	}
	mi.BaseDecimals, mi.QuoteDecimals = uint8(bd), uint8(qd)
	mi.MakerFeeBps, mi.TakerFeeBps, mi.BandBps = uint32(mk), uint32(tk), int64(band)
	mi.Halted = status == "HALTED"
	if mi.MinNotional, err = money.ParseInt128(minNotional); err != nil {
		return mi, fmt.Errorf("market %s min_notional: %w", mi.Symbol, err)
	}
	return mi, nil
}

// Refresh reloads all markets from the database.
func (ms *Markets) Refresh(ctx context.Context) error {
	rows, err := ms.pool.Query(ctx, `SELECT `+marketCols+` FROM markets ORDER BY symbol`)
	if err != nil {
		return err
	}
	defer rows.Close()
	m := map[string]marketInfo{}
	for rows.Next() {
		mi, err := scanMarket(rows)
		if err != nil {
			return err
		}
		m[mi.Symbol] = mi
	}
	if err := rows.Err(); err != nil {
		return err
	}
	ms.mu.Lock()
	ms.m = m
	ms.mu.Unlock()
	return nil
}

// Run refreshes periodically until ctx is cancelled.
func (ms *Markets) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := ms.Refresh(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("market refresh failed", "err", err)
			}
		}
	}
}

func (ms *Markets) get(symbol string) (marketInfo, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	mi, ok := ms.m[symbol]
	return mi, ok
}

// All returns all markets sorted by symbol.
func (ms *Markets) All() []marketInfo {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	out := make([]marketInfo, 0, len(ms.m))
	for _, mi := range ms.m {
		out = append(out, mi)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Symbol < out[j-1].Symbol; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// SetHalted halts or resumes a market.
func (ms *Markets) SetHalted(ctx context.Context, symbol string, halted bool) (marketInfo, error) {
	st := "ACTIVE"
	if halted {
		st = "HALTED"
	}
	mi, err := scanMarket(ms.pool.QueryRow(ctx,
		`UPDATE markets SET status = $2, updated_at = now() WHERE symbol = $1 RETURNING `+marketCols, symbol, st))
	if errors.Is(err, pgx.ErrNoRows) {
		return marketInfo{}, ErrMarketNotFound
	}
	if err != nil {
		return marketInfo{}, err
	}
	ms.mu.Lock()
	ms.m[symbol] = mi
	ms.mu.Unlock()
	return mi, nil
}
