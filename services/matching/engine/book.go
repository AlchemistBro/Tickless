package engine

// order is a resting order, linked into its price level's FIFO list.
type order struct {
	id, user   uint64
	side       Side
	price      int64
	qty        int64 // original quantity
	leaves     int64 // remaining quantity, always in (0, qty]
	ts         int64
	prev, next *order
	lvl        *level
	nextFree   *order
}

// level is a price level: FIFO queue of orders at one price.
type level struct {
	price      int64
	key        int64 // sort key: price for bids, -price for asks (best = max key)
	qty        int64
	head, tail *order
	nextFree   *level
}

func (l *level) push(o *order) {
	o.lvl = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.qty += o.leaves
}

func (l *level) unlink(o *order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	o.prev, o.next = nil, nil
}

// bookSide holds the levels of one side sorted ascending by key, so the best
// level is the LAST element. Matching and the common "insert near the top"
// both touch the tail, so slice shifts are short in practice (ADR 0002).
type bookSide struct {
	side   Side
	levels []*level
}

func (s *bookSide) keyOf(price int64) int64 {
	if s.side == SideBuy {
		return price
	}
	return -price
}

func (s *bookSide) best() *level {
	if n := len(s.levels); n > 0 {
		return s.levels[n-1]
	}
	return nil
}

// search returns the index of the first level whose key >= key. Hand-rolled
// binary search: sort.Search takes a closure, which would hurt the hot path.
func (s *bookSide) search(key int64) int {
	lo, hi := 0, len(s.levels)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.levels[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (s *bookSide) insertAt(i int, l *level) {
	s.levels = append(s.levels, nil)
	copy(s.levels[i+1:], s.levels[i:])
	s.levels[i] = l
}

func (s *bookSide) removeAt(i int) {
	copy(s.levels[i:], s.levels[i+1:])
	s.levels[len(s.levels)-1] = nil
	s.levels = s.levels[:len(s.levels)-1]
}

// pools recycle orders and levels in slabs so steady state never allocates.
type pools struct {
	orders *order
	levels *level
}

const slab = 1024

// reserve pre-populates the free lists with one slab each.
func (p *pools) reserve(orders, levels int) {
	oc := make([]order, orders)
	for i := range oc {
		oc[i].nextFree = p.orders
		p.orders = &oc[i]
	}
	lc := make([]level, levels)
	for i := range lc {
		lc[i].nextFree = p.levels
		p.levels = &lc[i]
	}
}

func (p *pools) getOrder() *order {
	if p.orders == nil {
		chunk := make([]order, slab)
		for i := range chunk {
			chunk[i].nextFree = p.orders
			p.orders = &chunk[i]
		}
	}
	o := p.orders
	p.orders = o.nextFree
	*o = order{}
	return o
}

func (p *pools) putOrder(o *order) {
	*o = order{}
	o.nextFree = p.orders
	p.orders = o
}

func (p *pools) getLevel() *level {
	if p.levels == nil {
		chunk := make([]level, slab)
		for i := range chunk {
			chunk[i].nextFree = p.levels
			p.levels = &chunk[i]
		}
	}
	l := p.levels
	p.levels = l.nextFree
	*l = level{}
	return l
}

func (p *pools) putLevel(l *level) {
	*l = level{}
	l.nextFree = p.levels
	p.levels = l
}
