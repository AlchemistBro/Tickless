package httpapi

import "github.com/tickless/pkg/money"

// assetDecimals returns the decimals of a known asset (0 and false if unknown).
func assetDecimals(symbol string) (uint8, bool) {
	a, ok := money.LookupAsset(symbol)
	return a.Decimals, ok
}

// formatUnits renders a smallest-unit integer string as a decimal amount of
// the asset. Unknown assets are returned in smallest units rather than guessed.
func formatUnits(units string, decimals uint8, known bool) string {
	if !known {
		return units
	}
	v, err := money.ParseInt128(units)
	if err != nil {
		return units
	}
	return money.Asset{Decimals: decimals}.Format(v)
}
