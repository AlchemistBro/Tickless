package money

import (
	"fmt"
	"math/big"
	"strings"
)

// MaxDecimals is the largest supported number of fractional digits.
const MaxDecimals = 36

// BasisPointsDen is the denominator for basis-point rates.
const BasisPointsDen = 10_000

// Asset describes a tradable/transferable asset. Amounts of the asset are
// Int128 counts of 10^-Decimals units.
type Asset struct {
	Symbol   string
	Decimals uint8
}

// Well-known assets of the local exchange.
var (
	BTC  = Asset{Symbol: "BTC", Decimals: 8}
	ETH  = Asset{Symbol: "ETH", Decimals: 18}
	USDT = Asset{Symbol: "USDT", Decimals: 6}
)

func pow10(n uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// Parse converts a decimal string such as "-12.345" to an amount in smallest
// units. Exponents, whitespace, separators and excess precision are rejected;
// we never silently truncate user input.
func (a Asset) Parse(s string) (Int128, error) {
	if a.Decimals > MaxDecimals {
		return Int128{}, fmt.Errorf("%w: decimals %d", ErrInvalid, a.Decimals)
	}
	intPart, frac, hasDot := strings.Cut(s, ".")
	if hasDot && frac == "" {
		return Int128{}, ErrInvalid
	}
	if hasDot && (frac[0] == '-' || frac[0] == '+') {
		return Int128{}, ErrInvalid
	}
	if intPart == "" || intPart == "-" || intPart == "+" {
		return Int128{}, ErrInvalid
	}
	if len(frac) > int(a.Decimals) {
		return Int128{}, ErrPrecision
	}
	if !isInteger(intPart) {
		return Int128{}, ErrInvalid
	}
	if hasDot && !isInteger("0"+frac) {
		return Int128{}, ErrInvalid
	}
	neg := intPart[0] == '-'
	digits := strings.TrimLeft(intPart, "+-") + frac + strings.Repeat("0", int(a.Decimals)-len(frac))
	z, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Int128{}, ErrInvalid
	}
	if neg {
		z.Neg(z)
	}
	return FromBig(z)
}

// Format renders an amount as a canonical decimal string with trailing
// fractional zeros removed ("1.5", "0", "-0.000001").
func (a Asset) Format(v Int128) string {
	z := v.Big()
	neg := z.Sign() < 0
	z.Abs(z)
	q, r := new(big.Int).QuoRem(z, pow10(a.Decimals), new(big.Int))
	out := q.String()
	if r.Sign() != 0 {
		frac := r.String()
		frac = strings.Repeat("0", int(a.Decimals)-len(frac)) + frac
		out += "." + strings.TrimRight(frac, "0")
	}
	if neg {
		out = "-" + out
	}
	return out
}

// Fee returns ceil(amount * bps / 10000): fees always round toward the
// exchange, so the exchange never under-collects (SPEC §6 invariant 8).
// amount must be non-negative.
func Fee(amount Int128, bps uint32) (Int128, error) {
	if amount.Sign() < 0 {
		return Int128{}, ErrInvalid
	}
	return MulDiv(amount, FromUint64(uint64(bps)), FromInt64(BasisPointsDen), RoundUp)
}

// MarketSpec is the exact-integer description of a market's grid.
// A price of P ticks means P*TickSize quote-units per 1 whole base unit
// (10^BaseDecimals base-units). A quantity of Q lots is Q*LotSize base-units.
type MarketSpec struct {
	BaseDecimals uint8
	TickSize     int64 // quote smallest units per tick
	LotSize      int64 // base smallest units per lot
}

// QuoteAmount returns the quote-asset amount for qtyLots at priceTicks:
//
//	priceTicks*TickSize * qtyLots*LotSize / 10^BaseDecimals
//
// rounded per mode. Callers pick the mode: holds use RoundUp (never
// under-reserve), trade settlement uses one mode for both legs so the journal
// entry balances. See ADR 0001.
func (m MarketSpec) QuoteAmount(priceTicks, qtyLots int64, mode Rounding) (Int128, error) {
	if priceTicks < 0 || qtyLots < 0 {
		return Int128{}, ErrInvalid
	}
	pricePerBase, err := FromInt64(priceTicks).Mul(FromInt64(m.TickSize))
	if err != nil {
		return Int128{}, err
	}
	baseUnits, err := FromInt64(qtyLots).Mul(FromInt64(m.LotSize))
	if err != nil {
		return Int128{}, err
	}
	den, err := FromBig(pow10(m.BaseDecimals))
	if err != nil {
		return Int128{}, err
	}
	return MulDiv(pricePerBase, baseUnits, den, mode)
}

// BaseAmount returns qtyLots*LotSize in base smallest units.
func (m MarketSpec) BaseAmount(qtyLots int64) (Int128, error) {
	return FromInt64(qtyLots).Mul(FromInt64(m.LotSize))
}
