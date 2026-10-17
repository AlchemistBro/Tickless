//go:build integration

package store_test

import (
	"context"
	"math/big"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/candles/internal/store"
	"github.com/tickless/pkg/testkit"
)

func open(t *testing.T) (*store.Store, store.Options, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	addr, user, pass, db := testkit.ClickHouse(t)
	o := store.Options{Addr: addr, Database: db, User: user, Password: pass}
	s, err := store.Open(ctx, o)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, o, ctx
}

// genTrades makes `n` trades over ~3 days with irregular gaps (including equal
// timestamps and a long quiet period) and ascending trade ids.
func genTrades(seed int64, market string, n int, startID uint64, start time.Time) []store.Trade {
	r := rand.New(rand.NewSource(seed))
	ts := start
	price := int64(5_000_000)
	var out []store.Trade
	for i := 0; i < n; i++ {
		switch r.Intn(10) {
		case 0:
			// same timestamp as the previous trade
		case 1:
			ts = ts.Add(time.Duration(2+r.Intn(5)) * time.Hour) // quiet period => empty buckets
		default:
			ts = ts.Add(time.Duration(r.Intn(90_000)) * time.Millisecond * 10)
		}
		price += int64(r.Intn(2001) - 1000)
		if price < 1000 {
			price = 1000
		}
		out = append(out, store.Trade{Market: market, TradeID: startID + uint64(i), Seq: uint64(i) * 3, Time: ts,
			Price: price, Qty: int64(1 + r.Intn(500_000)), TakerBuy: r.Intn(2) == 0, TakerUser: 1, MakerUser: 2})
	}
	return out
}

// expected aggregates trades in Go, independently of ClickHouse.
func expected(trades []store.Trade, interval string) []store.Candle {
	secs := store.Intervals[interval].Seconds
	type acc struct {
		c        store.Candle
		turnover *big.Int
		firstKey [2]int64
		lastKey  [2]int64
	}
	m := map[int64]*acc{}
	for _, t := range trades {
		b := t.Time.Unix() / secs * secs
		key := [2]int64{t.Time.UnixNano(), int64(t.TradeID)}
		a := m[b]
		if a == nil {
			a = &acc{c: store.Candle{OpenTime: b, Open: t.Price, High: t.Price, Low: t.Price, Close: t.Price}, turnover: new(big.Int), firstKey: key, lastKey: key}
			m[b] = a
		}
		if key[0] < a.firstKey[0] || (key[0] == a.firstKey[0] && key[1] < a.firstKey[1]) {
			a.firstKey, a.c.Open = key, t.Price
		}
		if key[0] > a.lastKey[0] || (key[0] == a.lastKey[0] && key[1] > a.lastKey[1]) {
			a.lastKey, a.c.Close = key, t.Price
		}
		a.c.High, a.c.Low = max(a.c.High, t.Price), min(a.c.Low, t.Price)
		a.c.Lots += t.Qty
		a.c.Trades++
		a.turnover.Add(a.turnover, new(big.Int).Mul(big.NewInt(t.Price), big.NewInt(t.Qty)))
	}
	var out []store.Candle
	for _, a := range m {
		a.c.Turnover = a.turnover.String()
		out = append(out, a.c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenTime > out[j].OpenTime }) // newest first
	return out
}

// Acceptance: candles of every interval equal an independent aggregation of
// the stored trades (OHLC with equal-timestamp tie-breaks, volume, turnover
// beyond int64, trade count), and markets do not mix.
func TestCandlesOfAllIntervalsMatchTheTrades(t *testing.T) {
	s, _, ctx := open(t)
	start := time.Date(2026, 3, 1, 23, 58, 30, 0, time.UTC) // starts just before midnight to cross a day boundary
	btc := genTrades(1, "BTC-USDT", 1800, 1, start)         // < 2000 so every 1m candle fits one page
	eth := genTrades(2, "ETH-USDT", 900, 1, start.Add(17*time.Minute))

	for lo := 0; lo < len(btc); lo += 600 { // several blocks
		_, err := s.Insert(ctx, btc[lo:min(lo+600, len(btc))])
		require.NoError(t, err)
	}
	_, err := s.Insert(ctx, eth)
	require.NoError(t, err)

	for _, iv := range store.IntervalNames {
		for market, trades := range map[string][]store.Trade{"BTC-USDT": btc, "ETH-USDT": eth} {
			got, err := s.Candles(ctx, market, iv, 0, 0, 2000)
			require.NoError(t, err)
			want := expected(trades, iv)
			require.Len(t, got, len(want), "%s %s candle count", market, iv)
			for i := range want {
				assert.Equal(t, want[i], got[i], "%s %s candle %d", market, iv, i)
			}
		}
	}
}

func TestInsertIsIdempotentPerMarketAndSurvivesRestart(t *testing.T) {
	s, o, ctx := open(t)
	start := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	trades := genTrades(3, "BTC-USDT", 400, 1, start)

	fresh, err := s.Insert(ctx, trades[:300])
	require.NoError(t, err)
	assert.Len(t, fresh, 300)

	// Redelivery of the same trades and an overlapping batch: only new trade ids are stored.
	fresh, err = s.Insert(ctx, trades[:300])
	require.NoError(t, err)
	assert.Empty(t, fresh)
	fresh, err = s.Insert(ctx, trades[200:])
	require.NoError(t, err)
	assert.Len(t, fresh, 100)

	// A restarted instance (empty cache) learns the high-water mark from ClickHouse.
	s2, err := store.Open(ctx, o)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()
	last, err := s2.LastTradeID(ctx, "BTC-USDT")
	require.NoError(t, err)
	assert.EqualValues(t, 400, last)
	fresh, err = s2.Insert(ctx, trades)
	require.NoError(t, err)
	assert.Empty(t, fresh)

	s.ForgetCache()
	fresh, err = s.Insert(ctx, trades)
	require.NoError(t, err)
	assert.Empty(t, fresh)

	// No double counting: candles still equal the aggregation of the 400 trades once.
	got, err := s.Candles(ctx, "BTC-USDT", "1h", 0, 0, 2000)
	require.NoError(t, err)
	assert.Equal(t, expected(trades, "1h"), got)
}

func TestCandleRangeLimitAndValidation(t *testing.T) {
	s, _, ctx := open(t)
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	trades := genTrades(4, "BTC-USDT", 800, 1, start)
	_, err := s.Insert(ctx, trades)
	require.NoError(t, err)

	all := expected(trades, "5m")
	require.Greater(t, len(all), 20)

	got, err := s.Candles(ctx, "BTC-USDT", "5m", 0, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, all[:10], got, "limit keeps the newest")

	from, to := all[15].OpenTime, all[5].OpenTime // [from, to) in ascending time
	got, err = s.Candles(ctx, "BTC-USDT", "5m", from, to, 2000)
	require.NoError(t, err)
	assert.Equal(t, all[6:16], got, "from inclusive, to exclusive, newest first")

	_, err = s.Candles(ctx, "BTC-USDT", "2m; DROP TABLE trades", 0, 0, 1)
	assert.Error(t, err, "interval is validated against a fixed set before touching SQL")
	empty, err := s.Candles(ctx, "NOPE", "1m", 0, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
