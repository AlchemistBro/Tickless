package money_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/money"
)

// fromParts builds a full-range Int128 from two int64 halves.
func fromParts(hi int64, lo uint64) money.Int128 {
	v := new(big.Int).SetInt64(hi)
	v.Lsh(v, 64)
	v.Add(v, new(big.Int).SetUint64(lo))
	r, err := money.FromBig(v)
	if err != nil {
		panic(err)
	}
	return r
}

var (
	refMax = money.MaxInt128.Big()
	refMin = money.MinInt128.Big()
)

func inRange(z *big.Int) bool { return z.Cmp(refMax) <= 0 && z.Cmp(refMin) >= 0 }

// FuzzArithmeticAgainstBig differential-tests Add/Sub/Mul/Cmp against math/big,
// including exact overflow detection.
func FuzzArithmeticAgainstBig(f *testing.F) {
	f.Add(int64(0), uint64(0), int64(0), uint64(1))
	f.Add(int64(1<<63-1), uint64(1<<64-1), int64(0), uint64(1))
	f.Add(int64(-1<<63), uint64(0), int64(-1), uint64(1<<64-1))
	f.Add(int64(-1), uint64(1<<64-1), int64(0), uint64(1))
	f.Fuzz(func(t *testing.T, ah int64, al uint64, bh int64, bl uint64) {
		a, b := fromParts(ah, al), fromParts(bh, bl)
		ba, bb := a.Big(), b.Big()

		require.Equal(t, ba.Cmp(bb), a.Cmp(b))

		sum := new(big.Int).Add(ba, bb)
		got, err := a.Add(b)
		if inRange(sum) {
			require.NoError(t, err)
			require.Zero(t, sum.Cmp(got.Big()))
		} else {
			require.ErrorIs(t, err, money.ErrOverflow)
		}

		diff := new(big.Int).Sub(ba, bb)
		got, err = a.Sub(b)
		if inRange(diff) {
			require.NoError(t, err)
			require.Zero(t, diff.Cmp(got.Big()))
		} else {
			require.ErrorIs(t, err, money.ErrOverflow)
		}

		prod := new(big.Int).Mul(ba, bb)
		got, err = a.Mul(b)
		if inRange(prod) {
			require.NoError(t, err)
			require.Zero(t, prod.Cmp(got.Big()))
		} else {
			require.ErrorIs(t, err, money.ErrOverflow)
		}
	})
}

// FuzzMulDiv checks floor/ceil semantics against an independent big.Int model.
func FuzzMulDiv(f *testing.F) {
	f.Add(int64(7), int64(3), int64(2))
	f.Add(int64(-7), int64(3), int64(2))
	f.Add(int64(7), int64(3), int64(-2))
	f.Add(int64(1<<62), int64(1<<62), int64(1))
	f.Fuzz(func(t *testing.T, a, b, c int64) {
		if c == 0 {
			_, err := money.MulDiv(money.FromInt64(a), money.FromInt64(b), money.FromInt64(c), money.RoundDown)
			require.ErrorIs(t, err, money.ErrDivByZero)
			return
		}
		num := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
		den := big.NewInt(c)
		// Reference floor: normalise den > 0 then use Euclidean division.
		n, d := new(big.Int).Set(num), new(big.Int).Set(den)
		if d.Sign() < 0 {
			n.Neg(n)
			d.Neg(d)
		}
		floor, rem := new(big.Int).DivMod(n, d, new(big.Int))
		ceil := new(big.Int).Set(floor)
		if rem.Sign() != 0 {
			ceil.Add(ceil, big.NewInt(1))
		}
		gd, err := money.MulDiv(money.FromInt64(a), money.FromInt64(b), money.FromInt64(c), money.RoundDown)
		require.NoError(t, err)
		gu, err := money.MulDiv(money.FromInt64(a), money.FromInt64(b), money.FromInt64(c), money.RoundUp)
		require.NoError(t, err)
		require.Zero(t, floor.Cmp(gd.Big()), "floor")
		require.Zero(t, ceil.Cmp(gu.Big()), "ceil")
	})
}

// FuzzFeeNeverUnderCollects: fee*10000 >= amount*bps and is minimal.
func FuzzFeeNeverUnderCollects(f *testing.F) {
	f.Add(int64(1001), uint32(10))
	f.Add(int64(1<<62), uint32(10000))
	f.Fuzz(func(t *testing.T, amount int64, bps uint32) {
		if amount < 0 {
			return
		}
		bps %= money.BasisPointsDen + 1
		fee, err := money.Fee(money.FromInt64(amount), bps)
		require.NoError(t, err)
		exact := new(big.Int).Mul(big.NewInt(amount), big.NewInt(int64(bps)))
		got := new(big.Int).Mul(fee.Big(), big.NewInt(money.BasisPointsDen))
		require.True(t, got.Cmp(exact) >= 0, "under-collected")
		less := new(big.Int).Sub(got, big.NewInt(money.BasisPointsDen))
		require.True(t, less.Cmp(exact) < 0, "over-collected by more than one unit")
	})
}

// FuzzParseFormat: Format(Parse(s)) is canonical and Parse(Format(v)) == v;
// Parse never panics on arbitrary input.
func FuzzParseFormat(f *testing.F) {
	for _, s := range []string{"0", "1.5", "-0.000001", "1e5", ".", "-", "1.2.3", "٣", "170141183460469231731687303715884105727"} {
		f.Add(s, uint8(6))
	}
	f.Fuzz(func(t *testing.T, s string, dec uint8) {
		asset := money.Asset{Symbol: "X", Decimals: dec % (money.MaxDecimals + 1)}
		v, err := asset.Parse(s)
		if err != nil {
			return
		}
		out := asset.Format(v)
		back, err := asset.Parse(out)
		require.NoError(t, err, "canonical output %q must reparse", out)
		require.Equal(t, v, back)
		require.Equal(t, out, asset.Format(back))
	})
}

// FuzzParseInt128 round-trips and must never panic.
func FuzzParseInt128(f *testing.F) {
	f.Add("123")
	f.Add("-170141183460469231731687303715884105728")
	f.Fuzz(func(t *testing.T, s string) {
		v, err := money.ParseInt128(s)
		if err != nil {
			return
		}
		back, err := money.ParseInt128(v.String())
		require.NoError(t, err)
		require.Equal(t, v, back)
	})
}
