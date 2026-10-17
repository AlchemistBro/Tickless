// Package state is the pure, deterministic market-data model: an order book
// maintained from BOOK_DELTA events, the last trades and a 24 h ticker kept in
// one-minute buckets. It has no I/O so it can be tested against the matching
// engine directly (an exact copy of the engine book is the acceptance test).
package state

import "sort"

// Level is an aggregated price level in ticks / lots.
type Level struct {
	Price int64 `json:"p"`
	Qty   int64 `json:"q"`
}

// Book holds both sides, each sorted by ascending price.
type Book struct {
	Bids []Level `json:"bids"`
	Asks []Level `json:"asks"`
}

func (b *Book) side(buy bool) *[]Level {
	if buy {
		return &b.Bids
	}
	return &b.Asks
}

// Set makes the total quantity at price equal to qty (0 removes the level).
func (b *Book) Set(buy bool, price, qty int64) {
	s := b.side(buy)
	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].Price >= price })
	found := i < len(*s) && (*s)[i].Price == price
	switch {
	case qty == 0 && found:
		*s = append((*s)[:i], (*s)[i+1:]...)
	case qty == 0:
	case found:
		(*s)[i].Qty = qty
	default:
		*s = append(*s, Level{})
		copy((*s)[i+1:], (*s)[i:])
		(*s)[i] = Level{Price: price, Qty: qty}
	}
}

// Best returns the best-first levels of a side (bids high to low, asks low to
// high), at most n of them (n <= 0 means all). The result is a copy.
func (b *Book) Best(buy bool, n int) []Level {
	s := *b.side(buy)
	if n <= 0 || n > len(s) {
		n = len(s)
	}
	out := make([]Level, 0, n)
	if buy {
		for i := len(s) - 1; i >= 0 && len(out) < n; i-- {
			out = append(out, s[i])
		}
	} else {
		out = append(out, s[:n]...)
	}
	return out
}

// Clone deep-copies the book.
func (b *Book) Clone() Book {
	return Book{Bids: append([]Level(nil), b.Bids...), Asks: append([]Level(nil), b.Asks...)}
}
