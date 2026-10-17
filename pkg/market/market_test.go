package market_test

import (
	"testing"

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
