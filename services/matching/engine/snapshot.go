package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// Snapshot wire format (big endian):
//
//	"TLSN" | u16 version | i64 offset | canonical | sha256(everything before it)
//
// canonical = u16 len(market) | market | u64 seq | u64 tradeSeq | u32 n |
// n * (u64 id | u64 user | u8 side | i64 price | i64 qty | i64 leaves | i64 ts)
//
// Orders are listed bids (worst->best level), then asks (worst->best level),
// each level in FIFO order, so re-inserting them in sequence reproduces the
// exact book including time priority. The trailing checksum protects the whole
// file; StateHash (the book identity used to compare runs) covers the
// canonical part only, so it does not depend on where a snapshot was taken.
const (
	snapMagic   = "TLSN"
	snapVersion = 1
	orderBytes  = 8 + 8 + 1 + 8 + 8 + 8 + 8
	maxSnapOrds = 1 << 28
)

// ErrCorruptSnapshot is returned by Restore for any malformed snapshot.
var ErrCorruptSnapshot = errors.New("engine: corrupt snapshot")

func (e *Engine) canonical(buf []byte) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.market)))
	buf = append(buf, e.market...)
	buf = binary.BigEndian.AppendUint64(buf, e.seq)
	buf = binary.BigEndian.AppendUint64(buf, e.tradeSeq)
	buf = binary.BigEndian.AppendUint32(buf, uint32(e.idx.count))
	for _, s := range []*bookSide{&e.bids, &e.asks} {
		for _, l := range s.levels {
			for o := l.head; o != nil; o = o.next {
				buf = binary.BigEndian.AppendUint64(buf, o.id)
				buf = binary.BigEndian.AppendUint64(buf, o.user)
				buf = append(buf, byte(o.side))
				buf = binary.BigEndian.AppendUint64(buf, uint64(o.price))
				buf = binary.BigEndian.AppendUint64(buf, uint64(o.qty))
				buf = binary.BigEndian.AppendUint64(buf, uint64(o.leaves))
				buf = binary.BigEndian.AppendUint64(buf, uint64(o.ts))
			}
		}
	}
	return buf
}

// StateHash is the sha256 of the canonical state: sequence counters and the
// full book with time priority. Two engines with equal hashes will produce
// identical output for any identical future command stream.
func (e *Engine) StateHash() [32]byte {
	return sha256.Sum256(e.canonical(nil))
}

// Snapshot serialises the engine. offset is the position of the next command
// to consume in the (Kafka) command log; it is returned verbatim by Restore.
func (e *Engine) Snapshot(offset int64) []byte {
	body := e.canonical(nil)
	buf := make([]byte, 0, len(snapMagic)+2+8+len(body)+sha256.Size)
	buf = append(buf, snapMagic...)
	buf = binary.BigEndian.AppendUint16(buf, snapVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(offset))
	buf = append(buf, body...)
	sum := sha256.Sum256(buf) // covers magic, version, offset and the canonical state
	return append(buf, sum[:]...)
}

// Restore rebuilds an engine from a snapshot and returns the stored offset.
// It validates the checksum, every field and the book invariants; it never panics.
func Restore(data []byte) (*Engine, int64, error) {
	const hdr = len(snapMagic) + 2 + 8
	if len(data) < hdr+sha256.Size || !bytes.HasPrefix(data, []byte(snapMagic)) {
		return nil, 0, fmt.Errorf("%w: header", ErrCorruptSnapshot)
	}
	if v := binary.BigEndian.Uint16(data[4:6]); v != snapVersion {
		return nil, 0, fmt.Errorf("%w: version %d", ErrCorruptSnapshot, v)
	}
	offset := int64(binary.BigEndian.Uint64(data[6:14]))
	body := data[hdr : len(data)-sha256.Size]
	if sum := sha256.Sum256(data[:len(data)-sha256.Size]); !bytes.Equal(sum[:], data[len(data)-sha256.Size:]) {
		return nil, 0, fmt.Errorf("%w: checksum", ErrCorruptSnapshot)
	}

	r := reader{b: body}
	mlen := int(r.u16())
	market := string(r.take(mlen))
	e := New(market)
	e.seq, e.tradeSeq = r.u64(), r.u64()
	n := r.u32()
	if r.err != nil || n > maxSnapOrds || uint64(len(r.b)) != uint64(n)*orderBytes {
		return nil, 0, fmt.Errorf("%w: body", ErrCorruptSnapshot)
	}
	for i := uint32(0); i < n; i++ {
		id, user := r.u64(), r.u64()
		side := Side(r.take(1)[0])
		price, qty, leaves, ts := int64(r.u64()), int64(r.u64()), int64(r.u64()), int64(r.u64())
		if (side != SideBuy && side != SideSell) || price <= 0 || qty <= 0 || leaves <= 0 || leaves > qty {
			return nil, 0, fmt.Errorf("%w: order %d fields", ErrCorruptSnapshot, id)
		}
		if e.idx.get(id) != nil {
			return nil, 0, fmt.Errorf("%w: duplicate order %d", ErrCorruptSnapshot, id)
		}
		o := e.pool.getOrder()
		o.id, o.user, o.side, o.price, o.qty, o.leaves, o.ts = id, user, side, price, qty, leaves, ts
		s := &e.bids
		if side == SideSell {
			s = &e.asks
		}
		e.levelFor(s, price).push(o)
		e.idx.put(o)
	}
	if err := e.CheckInvariants(); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrCorruptSnapshot, err)
	}
	// Level order and FIFO order must round-trip exactly.
	if !bytes.Equal(e.canonical(nil), body) {
		return nil, 0, fmt.Errorf("%w: non-canonical order", ErrCorruptSnapshot)
	}
	return e, offset, nil
}

// reader is a bounds-checked big-endian reader; after the first short read
// every call returns zero values and err is set.
type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil || len(r.b) < n {
		r.err = ErrCorruptSnapshot
		return make([]byte, n) // n <= 65535 (a u16 length), zero-filled
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u16() uint16 { return binary.BigEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.BigEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.BigEndian.Uint64(r.take(8)) }
