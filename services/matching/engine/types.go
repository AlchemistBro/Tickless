// Package engine is the Tickless matching engine: a deterministic,
// single-threaded state machine for ONE market.
//
// Input is an ordered stream of Commands, output an ordered stream of Events.
// The engine never reads a clock, never iterates a Go map, never locks and,
// after warm-up, never allocates. Prices are int64 ticks and quantities int64
// lots (ADR 0001); the engine never multiplies price by quantity.
// See docs/adr/0002-matching-engine-concurrency-model.md and
// docs/adr/0003-event-log-and-recovery.md.
package engine

import "math"

// Side of an order.
type Side uint8

// Order sides.
const (
	SideBuy  Side = 1
	SideSell Side = 2
)

// OrderType distinguishes limit from market orders.
type OrderType uint8

// Order types. A market order is an IOC/FOK order whose Price is a protective
// bound (0 = unbounded), which lets the orders service hold a bounded quote
// amount for market buys.
const (
	Limit  OrderType = 1
	Market OrderType = 2
)

// TIF is the time in force.
type TIF uint8

// Times in force. The zero value defaults to GTC for limit and IOC for market.
const (
	GTC TIF = 1
	IOC TIF = 2
	FOK TIF = 3
)

// CmdType is the kind of command.
type CmdType uint8

// Command kinds. Cancel-all is expanded into single cancels by the orders
// service, which owns the list of a user's open orders.
const (
	CmdPlace  CmdType = 1
	CmdCancel CmdType = 2
)

// Command is one input to the engine.
type Command struct {
	Type      CmdType
	OrderID   uint64
	UserID    uint64
	Side      Side
	OrderType OrderType
	TIF       TIF
	PostOnly  bool
	// Price in ticks. Limit: the limit price (>0). Market: protective bound, 0 = none.
	Price int64
	// Qty in lots (>0). Ignored for CmdCancel.
	Qty int64
	// Time is an opaque timestamp stamped by the producer; copied to events.
	Time int64
}

// EventType is the kind of event.
type EventType uint8

// Event kinds.
const (
	// EvAccepted: a place command passed validation. Every accepted order later
	// gets exactly one terminal outcome: fully filled via trades, EvCancelled,
	// or it rests until cancelled/filled.
	EvAccepted EventType = 1
	// EvRejected: the command had no effect on the book.
	EvRejected EventType = 2
	// EvTrade: a fill between a taker (OrderID) and a maker (MakerOrderID) at the maker's price.
	EvTrade EventType = 3
	// EvCancelled: remaining quantity (Qty) of OrderID leaves the market.
	EvCancelled EventType = 4
	// EvBookDelta: the total resting quantity at (Side, Price) is now Qty (0 = level removed).
	EvBookDelta EventType = 5
)

// Reason qualifies EvRejected and EvCancelled.
type Reason uint8

// Reasons.
const (
	ReasonNone Reason = iota
	RejInvalidQty
	RejInvalidPrice
	RejInvalidSide
	RejInvalidCombo
	RejDuplicateOrder
	RejPostOnlyWouldCross
	RejUnknownOrder
	RejUnknownCommand
	CancelUser
	CancelIOCRemainder
	CancelFOKUnfilled
	CancelSelfTrade
	RejInvalidOrderID
)

// Event is one output of the engine. It is a flat struct so that appending it
// to a caller-owned slice does not allocate.
type Event struct {
	Seq    uint64 // per-market, strictly increasing by 1
	Time   int64  // copied from the command that caused the event
	Type   EventType
	Reason Reason
	// Side: the order's side (Accepted/Rejected/Cancelled), the taker's side
	// (Trade) or the book side of the level (BookDelta).
	Side Side
	// OrderID/UserID: the order concerned; for trades, the taker.
	OrderID uint64
	UserID  uint64
	// MakerOrderID/MakerUserID: trades only.
	MakerOrderID uint64
	MakerUserID  uint64
	// Price: limit price (Accepted), execution price (Trade), level price (BookDelta).
	Price int64
	// Qty: order quantity (Accepted), fill quantity (Trade), remaining released
	// quantity (Cancelled), new level total (BookDelta).
	Qty int64
	// TakerLeaves/MakerLeaves: remaining quantity after the fill (Trade only).
	TakerLeaves int64
	MakerLeaves int64
	// TradeID: per-market trade counter (Trade only).
	TradeID uint64
}

const maxPrice = math.MaxInt64
