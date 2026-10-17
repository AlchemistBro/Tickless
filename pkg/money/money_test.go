package money_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/money"
)

func mustParse(t testing.TB, s string) money.Int128 {
	t.Helper()
	v, err := money.ParseInt128(s)
	require.NoError(t, err)
	return v
}

func TestInt128Bounds(t *testing.T) {
	assert.Equal(t, "170141183460469231731687303715884105727", money.MaxInt128.String())
	assert.Equal(t, "-170141183460469231731687303715884105728", money.MinInt128.String())

	_, err := money.MaxInt128.Add(money.FromInt64(1))
	assert.ErrorIs(t, err, money.ErrOverflow)
	_, err = money.MinInt128.Sub(money.FromInt64(1))
	assert.ErrorIs(t, err, money.ErrOverflow)
	_, err = money.MinInt128.Neg()
	assert.ErrorIs(t, err, money.ErrOverflow)
	_, err = money.MaxInt128.Mul(money.FromInt64(2))
	assert.ErrorIs(t, err, money.ErrOverflow)

	v, err := money.MaxInt128.Sub(money.MaxInt128)
	require.NoError(t, err)
	assert.True(t, v.IsZero())
}

func TestInt128CarryAcrossWords(t *testing.T) {
	a := mustParse(t, "18446744073709551615") // 2^64-1
	s, err := a.Add(money.FromInt64(1))
	require.NoError(t, err)
	assert.Equal(t, "18446744073709551616", s.String())
	d, err := s.Sub(money.FromInt64(1))
	require.NoError(t, err)
	assert.Equal(t, a, d)
	n := money.FromInt64(-1)
	z, err := n.Add(money.FromInt64(1))
	require.NoError(t, err)
	assert.True(t, z.IsZero())
}

func TestInt64RoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 1<<63 - 1, -1 << 63} {
		got, ok := money.FromInt64(v).Int64()
		require.True(t, ok)
		assert.Equal(t, v, got)
	}
	_, ok := money.MaxInt128.Int64()
	assert.False(t, ok)
	big64, _ := money.FromInt64(1<<63 - 1).Add(money.FromInt64(1))
	_, ok = big64.Int64()
	assert.False(t, ok)
}

func TestParseInt128Rejects(t *testing.T) {
	for _, s := range []string{"", "-", "+", "1.0", "1e3", " 1", "1 ", "1_000", "0x10", "--1", "٣"} {
		_, err := money.ParseInt128(s)
		assert.ErrorIs(t, err, money.ErrInvalid, "input %q", s)
	}
	_, err := money.ParseInt128("170141183460469231731687303715884105728")
	assert.ErrorIs(t, err, money.ErrOverflow)
}

func TestAssetParseFormat(t *testing.T) {
	cases := []struct {
		asset money.Asset
		in    string
		units string
		out   string
	}{
		{money.BTC, "1", "100000000", "1"},
		{money.BTC, "0.00000001", "1", "0.00000001"},
		{money.BTC, "-0.5", "-50000000", "-0.5"},
		{money.BTC, "21000000.12345678", "2100000012345678", "21000000.12345678"},
		{money.USDT, "0.1", "100000", "0.1"},
		{money.USDT, "000123.450000", "123450000", "123.45"},
		{money.ETH, "1.000000000000000001", "1000000000000000001", "1.000000000000000001"},
		{money.ETH, "+2", "2000000000000000000", "2"},
		{money.USDT, "0", "0", "0"},
		{money.USDT, "-0", "0", "0"},
	}
	for _, c := range cases {
		v, err := c.asset.Parse(c.in)
		require.NoError(t, err, c.in)
		assert.Equal(t, c.units, v.String(), c.in)
		assert.Equal(t, c.out, c.asset.Format(v), c.in)
	}
}

func TestAssetParseRejects(t *testing.T) {
	cases := map[string]error{
		"":          money.ErrInvalid,
		".5":        money.ErrInvalid,
		"1.":        money.ErrInvalid,
		"1e6":       money.ErrInvalid,
		"1,5":       money.ErrInvalid,
		"1.-5":      money.ErrInvalid,
		"1.+5":      money.ErrInvalid,
		"abc":       money.ErrInvalid,
		"NaN":       money.ErrInvalid,
		"0.0000001": money.ErrPrecision, // 7 digits on a 6-decimal asset
		"1.2.3":     money.ErrInvalid,
	}
	for in, want := range cases {
		_, err := money.USDT.Parse(in)
		assert.ErrorIs(t, err, want, "input %q", in)
	}
}

func TestFeeRoundsTowardExchange(t *testing.T) {
	cases := []struct {
		amount int64
		bps    uint32
		want   int64
	}{
		{0, 10, 0},
		{1, 10, 1},        // 0.001 -> 1
		{1000, 10, 1},     // exact
		{1001, 10, 2},     // 1.001 -> 2
		{10_000, 0, 0},    // zero fee
		{99_999, 25, 250}, // 249.9975 -> 250
		{1_000_000, 10_000, 1_000_000},
	}
	for _, c := range cases {
		got, err := money.Fee(money.FromInt64(c.amount), c.bps)
		require.NoError(t, err)
		assert.Equal(t, money.FromInt64(c.want), got, "%d@%dbps", c.amount, c.bps)
	}
	_, err := money.Fee(money.FromInt64(-1), 10)
	assert.ErrorIs(t, err, money.ErrInvalid)
}

func TestMulDivRounding(t *testing.T) {
	m := money.FromInt64
	cases := []struct {
		a, b, c  int64
		down, up int64
	}{
		{7, 1, 2, 3, 4},
		{-7, 1, 2, -4, -3},
		{7, 1, -2, -4, -3},
		{-7, 1, -2, 3, 4},
		{6, 1, 2, 3, 3},
		{0, 5, 3, 0, 0},
	}
	for _, c := range cases {
		d, err := money.MulDiv(m(c.a), m(c.b), m(c.c), money.RoundDown)
		require.NoError(t, err)
		u, err := money.MulDiv(m(c.a), m(c.b), m(c.c), money.RoundUp)
		require.NoError(t, err)
		assert.Equal(t, m(c.down), d, "floor %d*%d/%d", c.a, c.b, c.c)
		assert.Equal(t, m(c.up), u, "ceil %d*%d/%d", c.a, c.b, c.c)
	}
	_, err := money.MulDiv(m(1), m(1), m(0), money.RoundDown)
	assert.ErrorIs(t, err, money.ErrDivByZero)
}

func TestMulDivWideIntermediate(t *testing.T) {
	// a*b exceeds 128 bits but the quotient fits.
	a := money.MaxInt128
	got, err := money.MulDiv(a, money.FromInt64(10_000), money.FromInt64(10_000), money.RoundDown)
	require.NoError(t, err)
	assert.Equal(t, a, got)
}

func TestQuoteAmount(t *testing.T) {
	// BTC-USDT: tick 0.01 USDT (10^4 units), lot 0.00001 BTC (10^3 units).
	m := money.MarketSpec{BaseDecimals: 8, TickSize: 10_000, LotSize: 1_000}
	// 50,000.00 USDT/BTC => 5,000,000 ticks; 0.5 BTC => 50,000 lots.
	got, err := m.QuoteAmount(5_000_000, 50_000, money.RoundDown)
	require.NoError(t, err)
	assert.Equal(t, "25000", money.USDT.Format(got))

	// Inexact: 1 lot at 1 tick = 10^4*10^3/10^8 = 0.1 unit.
	d, err := m.QuoteAmount(1, 1, money.RoundDown)
	require.NoError(t, err)
	u, err := m.QuoteAmount(1, 1, money.RoundUp)
	require.NoError(t, err)
	assert.Equal(t, money.FromInt64(0), d)
	assert.Equal(t, money.FromInt64(1), u)

	_, err = m.QuoteAmount(-1, 1, money.RoundDown)
	assert.ErrorIs(t, err, money.ErrInvalid)
}

func TestBigRoundTrip(t *testing.T) {
	for _, s := range []string{"0", "1", "-1", "18446744073709551616", "-18446744073709551617",
		money.MaxInt128.String(), money.MinInt128.String()} {
		v := mustParse(t, s)
		back, err := money.FromBig(v.Big())
		require.NoError(t, err)
		assert.Equal(t, v, back)
		z, _ := new(big.Int).SetString(s, 10)
		assert.Zero(t, z.Cmp(v.Big()))
	}
}
