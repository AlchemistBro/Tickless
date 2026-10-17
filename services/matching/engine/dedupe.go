package engine

// DefaultDedupWindow is how many recently seen order ids an engine remembers.
const DefaultDedupWindow = 1 << 18

// idRing remembers the last `window` order ids that passed duplicate checking.
// The upstream outbox delivers commands at least once; a duplicate normally
// arrives within seconds of the original, long before 262k newer commands
// have been processed, so a bounded window is enough to make duplicates of
// FINISHED orders harmless (duplicates of live orders are caught by the order
// index). Eviction order is insertion order, so the structure is
// deterministic and is part of the snapshot. See ADR 0004.
type idRing struct {
	ring []uint64 // insertion-ordered ids, capacity == window
	head int      // index of the oldest id
	n    int
	set  []uint64 // open addressing, 0 = empty (order id 0 is invalid)
	mask uint64
}

func newIDRing(window int) idRing {
	if window < 1 {
		window = 1
	}
	size := 8
	for size < window*2 {
		size <<= 1
	}
	return idRing{ring: make([]uint64, window), set: make([]uint64, size), mask: uint64(size - 1)}
}

func (r *idRing) window() int { return len(r.ring) }

func (r *idRing) has(id uint64) bool {
	for i := hashID(id) & r.mask; ; i = (i + 1) & r.mask {
		switch r.set[i] {
		case 0:
			return false
		case id:
			return true
		}
	}
}

// add records id (which must not be present), evicting the oldest if full.
func (r *idRing) add(id uint64) {
	if r.n == len(r.ring) {
		r.remove(r.ring[r.head])
		r.ring[r.head] = id
		r.head = (r.head + 1) % len(r.ring)
	} else {
		r.ring[(r.head+r.n)%len(r.ring)] = id
		r.n++
	}
	i := hashID(id) & r.mask
	for r.set[i] != 0 {
		i = (i + 1) & r.mask
	}
	r.set[i] = id
}

// each calls f for ids from oldest to newest.
func (r *idRing) each(f func(id uint64)) {
	for k := 0; k < r.n; k++ {
		f(r.ring[(r.head+k)%len(r.ring)])
	}
}

// remove deletes id from the set with backward-shift deletion (no tombstones).
func (r *idRing) remove(id uint64) {
	i := hashID(id) & r.mask
	for r.set[i] != id {
		if r.set[i] == 0 {
			return
		}
		i = (i + 1) & r.mask
	}
	j := i
	for {
		j = (j + 1) & r.mask
		v := r.set[j]
		if v == 0 {
			break
		}
		k := hashID(v) & r.mask
		if i <= j {
			if i < k && k <= j {
				continue
			}
		} else if i < k || k <= j {
			continue
		}
		r.set[i] = v
		i = j
	}
	r.set[i] = 0
}
