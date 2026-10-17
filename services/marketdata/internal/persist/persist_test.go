package persist_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/marketdata/internal/persist"
	"github.com/tickless/marketdata/internal/state"
)

func TestSaveLoadRoundTripKeepsOffsetAndMarketsTogether(t *testing.T) {
	mr := miniredis.RunT(t)
	s := persist.New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()

	got, err := s.Load(ctx, 3)
	require.NoError(t, err)
	assert.Nil(t, got, "no snapshot yet")

	m := state.New("BTC-USDT")
	m.Book.Set(true, 100, 5)
	m.Seq, m.BookSeq = 7, 7
	blob, err := m.Marshal()
	require.NoError(t, err)
	require.NoError(t, s.Save(ctx, 3, 42, [][]byte{blob}))

	got, err = s.Load(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.EqualValues(t, 42, got.Offset)
	require.Len(t, got.Markets, 1)
	back, err := state.Unmarshal(got.Markets[0])
	require.NoError(t, err)
	assert.Equal(t, m, back)

	other, err := s.Load(ctx, 4)
	require.NoError(t, err)
	assert.Nil(t, other, "partitions are independent")

	// Saving replaces atomically: the new offset and the new markets arrive together.
	require.NoError(t, s.Save(ctx, 3, 99, nil))
	got, _ = s.Load(ctx, 3)
	assert.EqualValues(t, 99, got.Offset)
	assert.Empty(t, got.Markets)
}

func TestLoadReportsCorruptSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	require.NoError(t, mr.Set("md:snap:p0", "{broken"))
	s := persist.New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	_, err := s.Load(context.Background(), 0)
	assert.ErrorContains(t, err, "corrupt marketdata snapshot of partition 0")
}
