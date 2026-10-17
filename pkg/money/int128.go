// Package money implements exact integer money arithmetic for Tickless.
//
// Floats are never used. Amounts are signed 128-bit integers counting the
// smallest unit of an asset (satoshi, wei, 1e-6 USDT). 128 bits hold any
// realistic wei balance (1e18 * 1e9 coins = 1e27 << 1.7e38) with checked
// overflow. See docs/adr/0001-money-representation.md.
package money

import (
	"errors"
	"math/big"
	"math/bits"
)

var (
	// ErrOverflow is returned when a result does not fit in Int128.
	ErrOverflow = errors.New("money: overflow")
	// ErrDivByZero is returned for a zero divisor.
	ErrDivByZero = errors.New("money: division by zero")
	// ErrInvalid is returned for malformed textual input.
	ErrInvalid = errors.New("money: invalid number")
	// ErrPrecision is returned when input has more fractional digits than the asset allows.
	ErrPrecision = errors.New("money: too many fractional digits")
)

// Int128 is a signed 128-bit integer in two's complement. The zero value is 0.
// All arithmetic is overflow-checked; there is no wrapping operation.
type Int128 struct {
	hi int64
	lo uint64
}

var (
	// MaxInt128 is 2^127-1.
	MaxInt128 = Int128{hi: 1<<63 - 1, lo: 1<<64 - 1}
	// MinInt128 is -2^127.
	MinInt128 = Int128{hi: -1 << 63, lo: 0}
)

// FromInt64 converts an int64.
func FromInt64(v int64) Int128 {
	return Int128{hi: v >> 63, lo: uint64(v)}
}

// FromUint64 converts a uint64.
func FromUint64(v uint64) Int128 { return Int128{lo: v} }

// Sign returns -1, 0 or +1.
func (a Int128) Sign() int {
	switch {
	case a.hi < 0:
		return -1
	case a.hi == 0 && a.lo == 0:
		return 0
	default:
		return 1
	}
}

// IsZero reports a == 0.
func (a Int128) IsZero() bool { return a.hi == 0 && a.lo == 0 }

// Cmp returns -1, 0, +1 for a<b, a==b, a>b.
func (a Int128) Cmp(b Int128) int {
	switch {
	case a.hi < b.hi:
		return -1
	case a.hi > b.hi:
		return 1
	case a.lo < b.lo:
		return -1
	case a.lo > b.lo:
		return 1
	}
	return 0
}

// Add returns a+b or ErrOverflow.
func (a Int128) Add(b Int128) (Int128, error) {
	lo, c := bits.Add64(a.lo, b.lo, 0)
	hi := int64(uint64(a.hi) + uint64(b.hi) + c)
	if (a.hi >= 0) == (b.hi >= 0) && (hi >= 0) != (a.hi >= 0) {
		return Int128{}, ErrOverflow
	}
	return Int128{hi: hi, lo: lo}, nil
}

// Sub returns a-b or ErrOverflow.
func (a Int128) Sub(b Int128) (Int128, error) {
	lo, br := bits.Sub64(a.lo, b.lo, 0)
	hi := int64(uint64(a.hi) - uint64(b.hi) - br)
	if (a.hi >= 0) != (b.hi >= 0) && (hi >= 0) != (a.hi >= 0) {
		return Int128{}, ErrOverflow
	}
	return Int128{hi: hi, lo: lo}, nil
}

// Neg returns -a or ErrOverflow for MinInt128.
func (a Int128) Neg() (Int128, error) { return Int128{}.Sub(a) }

// Abs returns |a| or ErrOverflow for MinInt128.
func (a Int128) Abs() (Int128, error) {
	if a.hi < 0 {
		return a.Neg()
	}
	return a, nil
}

// Int64 returns the value as int64 and whether it fits.
func (a Int128) Int64() (int64, bool) {
	if a.hi == int64(a.lo)>>63 {
		return int64(a.lo), true
	}
	return 0, false
}

// Big returns the value as a new *big.Int.
func (a Int128) Big() *big.Int {
	z := new(big.Int).SetInt64(a.hi)
	z.Lsh(z, 64)
	return z.Add(z, new(big.Int).SetUint64(a.lo))
}

var (
	bigMax = MaxInt128.Big()
	bigMin = MinInt128.Big()
)

// FromBig converts a *big.Int, returning ErrOverflow if out of range.
func FromBig(z *big.Int) (Int128, error) {
	if z.Cmp(bigMax) > 0 || z.Cmp(bigMin) < 0 {
		return Int128{}, ErrOverflow
	}
	// Two's complement split: hi = floor(z / 2^64), lo = z mod 2^64.
	hi := new(big.Int).Rsh(z, 64) // arithmetic shift == floor division
	lo := new(big.Int).Sub(z, new(big.Int).Lsh(hi, 64))
	return Int128{hi: hi.Int64(), lo: lo.Uint64()}, nil
}

// Mul returns a*b or ErrOverflow. Not intended for the matching hot path
// (the engine works on int64 ticks/lots); correctness over speed.
func (a Int128) Mul(b Int128) (Int128, error) {
	return FromBig(new(big.Int).Mul(a.Big(), b.Big()))
}

// String returns the base-10 representation.
func (a Int128) String() string { return a.Big().String() }

// ParseInt128 parses an optionally signed base-10 integer. Whitespace,
// underscores, exponents and decimal points are rejected.
func ParseInt128(s string) (Int128, error) {
	if !isInteger(s) {
		return Int128{}, ErrInvalid
	}
	z, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return Int128{}, ErrInvalid
	}
	return FromBig(z)
}

func isInteger(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Rounding selects the direction used when a division is inexact.
type Rounding uint8

const (
	// RoundDown rounds toward negative infinity (floor).
	RoundDown Rounding = iota
	// RoundUp rounds toward positive infinity (ceil).
	RoundUp
)

// MulDiv computes a*b/c with the 256-bit-safe intermediate product, rounding
// per mode. Used for fees and notional values.
func MulDiv(a, b, c Int128, mode Rounding) (Int128, error) {
	if c.IsZero() {
		return Int128{}, ErrDivByZero
	}
	num := new(big.Int).Mul(a.Big(), b.Big())
	q, r := new(big.Int).DivMod(num, c.Big(), new(big.Int)) // Euclidean: r >= 0
	if c.Sign() < 0 {
		// Euclidean q is ceil for negative divisors; convert to floor semantics.
		if r.Sign() != 0 {
			q.Sub(q, big.NewInt(1))
		}
	}
	// q is now floor(num/c).
	if mode == RoundUp && r.Sign() != 0 {
		q.Add(q, big.NewInt(1))
	}
	return FromBig(q)
}

// DivRem divides a by d (d != 0) truncating toward zero and returns the
// quotient and remainder (remainder has the sign of a).
func (a Int128) DivRem(d Int128) (q, r Int128, err error) {
	if d.IsZero() {
		return Int128{}, Int128{}, ErrDivByZero
	}
	bq, br := new(big.Int).QuoRem(a.Big(), d.Big(), new(big.Int))
	if q, err = FromBig(bq); err != nil {
		return Int128{}, Int128{}, err
	}
	r, err = FromBig(br)
	return q, r, err
}
