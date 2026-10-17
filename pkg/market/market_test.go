package market_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
)

// BTC-USDT: tick 0.01 USDT, lot 0.00001 BTC.
var btcUsdt = market.Spec{
	Symbol: "BTC-USDT", Base: "BTC", Quote: "USDT", BaseDecimals: 8, QuoteDecimals: 6,
	TickSize: 10_000, LotSize: 1_000,
}

func TestParseFormatRoundTrip(t *testing.T) {
	p, err := btcUsdt.ParsePrice("50000.25")
	require.NoError(t, err)
	assert.Equal(t, int64(5_000_025), p)
	assert.Equal(t, "50000.25", btcUsdt.FormatPrice(p))

	q, err := btcUsdt.ParseQty("0.5")
	require.NoError(t, err)
	assert.Equal(t, int64(50_000), q)
	assert.Equal(t, "0.5", btcUsdt.FormatQty(q))
}

func TestGridViolations(t *testing.T) {
	_, err := btcUsdt.ParsePrice("50000.255")
	assert.ErrorIs(t, err, market.ErrOffTick)
	_, err = btcUsdt.ParseQty("0.000001")
	assert.ErrorIs(t, err, market.ErrOffLot)
	_, err = btcUsdt.ParsePrice("0")
	assert.ErrorIs(t, err, market.ErrNonPos)
	_, err = btcUsdt.ParseQty("-1")
	assert.ErrorIs(t, err, market.ErrNonPos)
	_, err = btcUsdt.ParsePrice("1e3")
	assert.ErrorIs(t, err, money.ErrInvalid)
	_, err = btcUsdt.ParsePrice("0.0000001")
	assert.ErrorIs(t, err, money.ErrPrecision)
	_, err = btcUsdt.ParseQty("99999999999999999999")
	assert.Error(t, err)
}

func TestNotional(t *testing.T) {
	n, err := btcUsdt.Notional(5_000_025, 50_000, money.RoundDown)
	require.NoError(t, err)
	assert.Equal(t, "25000.125", money.USDT.Format(n))
}

func TestProtoRoundTrip(t *testing.T) {
	spec := btcUsdt
	spec.MinNotional = money.FromInt64(5_000_000)
	spec.MakerFeeBps, spec.TakerFeeBps = 10, 20
	spec.MinPriceTicks, spec.MaxPriceTicks = 1, 1_000_000_000
	back, err := market.FromProto(spec.ToProto())
	require.NoError(t, err)
	assert.Equal(t, spec, back)

	spec.Halted = true
	back, err = market.FromProto(spec.ToProto())
	require.NoError(t, err)
	assert.True(t, back.Halted)

	bad := spec.ToProto()
	bad.MinNotional = "not-a-number"
	_, err = market.FromProto(bad)
	assert.Error(t, err)
}

func TestFormatEdgeCases(t *testing.T) {
	assert.Equal(t, "0.00001", btcUsdt.FormatQty(1))
	assert.Equal(t, "0.01", btcUsdt.FormatPrice(1))
	assert.Equal(t, "0", btcUsdt.FormatQty(0))
}

func TestCacheRefreshAndLookup(t *testing.T) {
	calls := 0
	c := market.NewCache(func(context.Context) ([]market.Spec, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("orders not up yet")
		}
		return []market.Spec{{Symbol: "B-X"}, {Symbol: "A-X"}}, nil
	})
	_, ok := c.Spec("A-X")
	assert.False(t, ok)
	select {
	case <-c.Ready():
		t.Fatal("ready before the first successful refresh")
	default:
	}
	require.Error(t, c.Refresh(context.Background()))
	require.NoError(t, c.Refresh(context.Background()))
	<-c.Ready()
	s, ok := c.Spec("A-X")
	assert.True(t, ok)
	assert.Equal(t, "A-X", s.Symbol)
	all := c.All()
	require.Len(t, all, 2)
	assert.Equal(t, "A-X", all[0].Symbol, "sorted by symbol")
}

func TestCacheRunRetriesUntilFirstSuccess(t *testing.T) {
	var n atomic.Int32
	c := market.NewCache(func(context.Context) ([]market.Spec, error) {
		if n.Add(1) < 3 {
			return nil, errors.New("down")
		}
		return []market.Spec{{Symbol: "A-X"}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, time.Hour)
	select {
	case <-c.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("cache never became ready")
	}
}
