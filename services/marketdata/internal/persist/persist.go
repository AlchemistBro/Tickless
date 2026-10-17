// Package persist stores marketdata snapshots in Redis so a restarted (or
// additional) instance resumes from the snapshot instead of replaying the
// whole event log.
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Snapshot is the state of every market of one Kafka partition at one offset.
type Snapshot struct {
	// Offset is the offset of the next matching.events record to consume.
	Offset  int64             `json:"offset"`
	Markets []json.RawMessage `json:"markets"`
}

// Store reads and writes snapshots.
type Store struct{ rdb redis.Cmdable }

// New returns a Store on rdb.
func New(rdb redis.Cmdable) *Store { return &Store{rdb: rdb} }

func key(partition int32) string { return fmt.Sprintf("md:snap:p%d", partition) }

// Save atomically replaces the snapshot of a partition. The offset and the
// markets are one value, so they can never disagree.
func (s *Store) Save(ctx context.Context, partition int32, offset int64, markets [][]byte) error {
	snap := Snapshot{Offset: offset, Markets: make([]json.RawMessage, len(markets))}
	for i, m := range markets {
		snap.Markets[i] = m
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, key(partition), b, 0).Err()
}

// Load returns the snapshot of a partition, or nil if there is none.
func (s *Store) Load(ctx context.Context, partition int32) (*Snapshot, error) {
	b, err := s.rdb.Get(ctx, key(partition)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, fmt.Errorf("corrupt marketdata snapshot of partition %d: %w", partition, err)
	}
	return &snap, nil
}
