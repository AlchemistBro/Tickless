// Package partition owns the matching engines of one Kafka partition.
//
// Markets are hashed to partitions by their Kafka key, so every market lives in
// exactly one partition and is only ever touched by that partition's single
// goroutine (ADR 0002). A checkpoint captures ALL engines of the partition at
// one consistent log offset, which makes "restore checkpoint, replay from
// offset" correct even for markets that never reached their own snapshot
// interval (ADR 0003).
package partition

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/tickless/matching/engine"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
)

// Partition holds the engines of the markets mapped to one partition.
type Partition struct {
	engines map[string]*engine.Engine
	buf     []engine.Event
}

// New returns an empty partition.
func New() *Partition { return &Partition{engines: map[string]*engine.Engine{}} }

// Result is one engine event tagged with its market.
type Result struct {
	Market string
	Event  engine.Event
}

// Apply feeds a command to the right engine (created on first use) and
// returns the events. The returned slice is valid until the next call.
func (p *Partition) Apply(c *matchingv1.Command, out []Result) []Result {
	e := p.engines[c.Market]
	if e == nil {
		e = engine.New(c.Market)
		p.engines[c.Market] = e
	}
	p.buf = e.Process(ToEngine(c), p.buf[:0])
	for _, ev := range p.buf {
		out = append(out, Result{Market: c.Market, Event: ev})
	}
	return out
}

// ToEngine converts a wire command. Unknown enum values are passed through
// unchanged so the engine rejects them deterministically.
func ToEngine(c *matchingv1.Command) engine.Command {
	return engine.Command{
		Type: engine.CmdType(c.Type), OrderID: c.OrderId, UserID: c.UserId, Side: engine.Side(c.Side),
		OrderType: engine.OrderType(c.OrderType), TIF: engine.TIF(c.Tif), PostOnly: c.PostOnly,
		Price: c.Price, Qty: c.Qty, Time: c.TimeUnixNano,
	}
}

// ToProto converts an engine event to its wire form.
func ToProto(market string, e engine.Event) *matchingv1.Event {
	return &matchingv1.Event{
		Market: market, Seq: e.Seq, TimeUnixNano: e.Time,
		Type: matchingv1.EventType(e.Type), Reason: matchingv1.Reason(e.Reason), Side: commonv1.Side(e.Side),
		OrderId: e.OrderID, UserId: e.UserID, MakerOrderId: e.MakerOrderID, MakerUserId: e.MakerUserID,
		Price: e.Price, Qty: e.Qty, TakerLeaves: e.TakerLeaves, MakerLeaves: e.MakerLeaves, TradeId: e.TradeID,
	}
}

// Markets returns the markets present, sorted.
func (p *Partition) Markets() []string {
	ms := make([]string, 0, len(p.engines))
	for m := range p.engines {
		ms = append(ms, m)
	}
	slices.Sort(ms)
	return ms
}

// Engine returns a market's engine (nil if unseen). For tests and metrics.
func (p *Partition) Engine(market string) *engine.Engine { return p.engines[market] }

// StateHash is a hash over all engine state hashes in market order.
func (p *Partition) StateHash() [32]byte {
	h := sha256.New()
	for _, m := range p.Markets() {
		sh := p.engines[m].StateHash()
		h.Write([]byte(m))
		h.Write(sh[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// Checkpoint serialises every engine; offset is the log offset of the next
// command to consume.
//
//	"TLCK" | i64 offset | u32 count | count * (u32 len | engine snapshot with that offset)
func (p *Partition) Checkpoint(offset int64) []byte {
	var b bytes.Buffer
	b.WriteString("TLCK")
	_ = binary.Write(&b, binary.BigEndian, offset)
	_ = binary.Write(&b, binary.BigEndian, uint32(len(p.engines)))
	for _, m := range p.Markets() {
		snap := p.engines[m].Snapshot(offset)
		_ = binary.Write(&b, binary.BigEndian, uint32(len(snap)))
		b.Write(snap)
	}
	return b.Bytes()
}

// ErrCorruptCheckpoint is returned for malformed checkpoints.
var ErrCorruptCheckpoint = errors.New("partition: corrupt checkpoint")

// Restore rebuilds a partition from a checkpoint and returns its offset.
func Restore(data []byte) (*Partition, int64, error) {
	if len(data) < 16 || string(data[:4]) != "TLCK" {
		return nil, 0, ErrCorruptCheckpoint
	}
	offset := int64(binary.BigEndian.Uint64(data[4:12]))
	n := binary.BigEndian.Uint32(data[12:16])
	rest := data[16:]
	p := New()
	for i := uint32(0); i < n; i++ {
		if len(rest) < 4 {
			return nil, 0, ErrCorruptCheckpoint
		}
		l := binary.BigEndian.Uint32(rest[:4])
		rest = rest[4:]
		if uint64(len(rest)) < uint64(l) {
			return nil, 0, ErrCorruptCheckpoint
		}
		e, off, err := engine.Restore(rest[:l])
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrCorruptCheckpoint, err)
		}
		rest = rest[l:]
		if off != offset {
			return nil, 0, fmt.Errorf("%w: engine offset %d != checkpoint offset %d", ErrCorruptCheckpoint, off, offset)
		}
		p.engines[e.Market()] = e
	}
	if len(rest) != 0 {
		return nil, 0, ErrCorruptCheckpoint
	}
	return p, offset, nil
}
