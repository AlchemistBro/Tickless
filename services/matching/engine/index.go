package engine

// orderIndex maps order id -> *order with open addressing, linear probing and
// backward-shift deletion. Unlike a Go map it never allocates except when it
// doubles (warm-up) and it has no tombstones, so churn is allocation-free.
// It is never iterated; iteration order would be non-deterministic.
type orderIndex struct {
	slots []*order
	mask  uint64
	count int
}

func newOrderIndex(capacity int) orderIndex {
	n := 1024
	for n < capacity*2 {
		n <<= 1
	}
	return orderIndex{slots: make([]*order, n), mask: uint64(n - 1)}
}

func hashID(id uint64) uint64 {
	id ^= id >> 33
	id *= 0xff51afd7ed558ccd
	id ^= id >> 33
	return id
}

func (x *orderIndex) get(id uint64) *order {
	for i := hashID(id) & x.mask; ; i = (i + 1) & x.mask {
		o := x.slots[i]
		if o == nil {
			return nil
		}
		if o.id == id {
			return o
		}
	}
}

func (x *orderIndex) put(o *order) {
	if (x.count+1)*2 > len(x.slots) {
		x.grow()
	}
	x.insert(o)
	x.count++
}

func (x *orderIndex) insert(o *order) {
	i := hashID(o.id) & x.mask
	for x.slots[i] != nil {
		i = (i + 1) & x.mask
	}
	x.slots[i] = o
}

func (x *orderIndex) grow() {
	old := x.slots
	x.slots = make([]*order, len(old)*2)
	x.mask = uint64(len(x.slots) - 1)
	for _, o := range old {
		if o != nil {
			x.insert(o)
		}
	}
}

func (x *orderIndex) del(id uint64) {
	i := hashID(id) & x.mask
	for {
		o := x.slots[i]
		if o == nil {
			return
		}
		if o.id == id {
			break
		}
		i = (i + 1) & x.mask
	}
	// Backward-shift: pull later members of the probe chain into the hole.
	j := i
	for {
		j = (j + 1) & x.mask
		o := x.slots[j]
		if o == nil {
			break
		}
		k := hashID(o.id) & x.mask
		// Skip if k lies cyclically in (i, j]: o is already reachable.
		if i <= j {
			if i < k && k <= j {
				continue
			}
		} else if i < k || k <= j {
			continue
		}
		x.slots[i] = o
		i = j
	}
	x.slots[i] = nil
	x.count--
}
