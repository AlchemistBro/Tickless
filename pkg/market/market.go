// Package market describes a trading market and converts between the decimal
// strings used at the API edge and the integer ticks/lots used by the matching
// engine (ADR 0001).
package market

import (
	"errors"
	"fmt"

	"github.com/tickless/pkg/money"
)

// Spec is the full configuration of a market.
type Spec struct {
	Symbol        string
	Base, Quote   string
	BaseDecimals  uint8
	QuoteDecimals uint8
	// TickSize is quote smallest units per price tick; LotSize is base
	// smallest units per lot.
	TickSize, LotSize int64
	// MinNotional is the minimum order value in quote smallest units.
	MinNotional money.Int128
	// Fees in basis points, charged in the asset the user receives.
	MakerFeeBps, TakerFeeBps uint32
	// MinPriceTicks/MaxPriceTicks bound resting prices; this bounds the number
	// of live price levels (ADR 0002).
	MinPriceTicks, MaxPriceTicks int64
	Halted                       bool
}

// Errors returned when user input does not fit the market grid.
var (
	ErrOffTick  = errors.New("market: price is not a multiple of the tick size")
	ErrOffLot   = errors.New("market: quantity is not a multiple of the lot size")
	ErrNonPos   = errors.New("market: value must be positive")
	ErrTooLarge = errors.New("market: value too large")
)

// Money returns the integer conversion spec.
func (s Spec) Money() money.MarketSpec {
	return money.MarketSpec{BaseDecimals: s.BaseDecimals, TickSize: s.TickSize, LotSize: s.LotSize}
}

func (s Spec) baseAsset() money.Asset { return money.Asset{Symbol: s.Base, Decimals: s.BaseDecimals} }
func (s Spec) quoteAsset() money.Asset {
	return money.Asset{Symbol: s.Quote, Decimals: s.QuoteDecimals}
}

// toGrid parses text in asset units and divides by step, requiring exactness.
func toGrid(a money.Asset, text string, step int64, offGrid error) (int64, error) {
	v, err := a.Parse(text)
	if err != nil {
		return 0, err
	}
	if v.Sign() <= 0 {
		return 0, ErrNonPos
	}
	q, r, err := v.DivRem(money.FromInt64(step))
	if err != nil {
		return 0, err
	}
	if !r.IsZero() {
		return 0, offGrid
	}
	n, ok := q.Int64()
	if !ok {
		return 0, ErrTooLarge
	}
	return n, nil
}

// ParsePrice converts "50000.25" (quote per base) to ticks.
func (s Spec) ParsePrice(text string) (int64, error) {
	return toGrid(s.quoteAsset(), text, s.TickSize, ErrOffTick)
}

// ParseQty converts "0.5" (base) to lots.
func (s Spec) ParseQty(text string) (int64, error) {
	return toGrid(s.baseAsset(), text, s.LotSize, ErrOffLot)
}

// FormatPrice renders ticks as a decimal price.
func (s Spec) FormatPrice(ticks int64) string {
	v, err := money.FromInt64(ticks).Mul(money.FromInt64(s.TickSize))
	if err != nil {
		return fmt.Sprintf("%d", ticks)
	}
	return s.quoteAsset().Format(v)
}

// FormatQty renders lots as a decimal quantity.
func (s Spec) FormatQty(lots int64) string {
	v, err := s.Money().BaseAmount(lots)
	if err != nil {
		return fmt.Sprintf("%d", lots)
	}
	return s.baseAsset().Format(v)
}

// Notional is the quote value of qtyLots at priceTicks, rounded per mode.
func (s Spec) Notional(priceTicks, qtyLots int64, mode money.Rounding) (money.Int128, error) {
	return s.Money().QuoteAmount(priceTicks, qtyLots, mode)
}
