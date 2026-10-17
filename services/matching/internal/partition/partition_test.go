package partition_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/matching/engine"
	"github.com/tickless/matching/internal/partition"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
)

func genStream(seed int64, n int) []*matchingv1.Command {
	r := rand.New(rand.NewSource(seed))
	markets := []string{"BTC-USDT", "ETH-USDT", "ETH-BTC"}
	var out []*matchingv1.Command
	var id uint64
	for i := 0; i < n; i++ {
		m := markets[r.Intn(len(markets))]
		if r.Intn(5) == 0 && id > 0 {
			out = append(out, &matchingv1.Command{Market: m, Type: matchingv1.CommandType_COMMAND_TYPE_CANCEL,
				OrderId: uint64(1 + r.Intn(int(id))), UserId: uint64(1 + r.Intn(4)), TimeUnixNano: int64(i)})
			continue
		}
		id++
		side := commonv1.Side_SIDE_BUY
		if r.Intn(2) == 0 {
			side = commonv1.Side_SIDE_SELL
		}
		tif := matchingv1.TimeInForce(1 + r.Intn(3))
		out = append(out, &matchingv1.Command{Market: m, Type: matchingv1.CommandType_COMMAND_TYPE_PLACE, OrderId: id,
			UserId: uint64(1 + r.Intn(4)), Side: side, OrderType: matchingv1.OrderType_ORDER_TYPE_LIMIT, Tif: tif,
			Price: int64(95 + r.Intn(11)), Qty: int64(1 + r.Intn(9)), TimeUnixNano: int64(i)})
	}
	return out
}

func run(p *partition.Partition, cmds []*matchingv1.Command) []partition.Result {
	var res []partition.Result
	for _, c := range cmds {
		res = p.Apply(c, res)
	}
	return res
}

// A checkpoint covers every market of the partition at one offset; restoring it
// and replaying the rest equals an uninterrupted run (SPEC §6 invariant 6).
func TestCheckpointRestoreReplayEqualsUninterrupted(t *testing.T) {
	cmds := genStream(1, 3000)
	ref := partition.New()
	refRes := run(ref, cmds)

	for _, cut := range []int{0, 1, 17, 999, 1500, 2999, 3000} {
		p := partition.New()
		firstRes := run(p, cmds[:cut])
		data := p.Checkpoint(int64(cut), 6)
		restored, off, parts, err := partition.Restore(data)
		require.NoError(t, err)
		require.Equal(t, int64(cut), off)
		require.Equal(t, 6, parts)
		require.Equal(t, p.StateHash(), restored.StateHash())

		rest := run(restored, cmds[cut:])
		want := refRes[len(firstRes):]
		require.Len(t, rest, len(want), "event count after cut %d", cut)
		if len(want) > 0 {
			assert.Equal(t, want, rest, "events after cut %d", cut)
		}
		assert.Equal(t, ref.StateHash(), restored.StateHash(), "final state after cut %d", cut)
	}
}

// Replaying commands that were already applied before the crash (because the
// last checkpoint is older than the crash point) re-emits identical events,
// so downstream dedupe on (market, seq) is exact.
func TestReplayAfterCrashReEmitsIdenticalEvents(t *testing.T) {
	cmds := genStream(2, 2000)
	p := partition.New()
	first := run(p, cmds[:1500]) // crashed after 1500 commands
	// last checkpoint was at 1000
	q := partition.New()
	_ = run(q, cmds[:1000])
	restored, off, _, err := partition.Restore(q.Checkpoint(1000, 6))
	require.NoError(t, err)
	replay := run(restored, cmds[off:1500])
	// the replayed events are exactly the tail of what the crashed process emitted
	assert.Equal(t, first[len(first)-len(replay):], replay)
}

func TestEmptyPartitionCheckpointKeepsOffset(t *testing.T) {
	p := partition.New()
	r, off, _, err := partition.Restore(p.Checkpoint(1234, 6))
	require.NoError(t, err)
	assert.Equal(t, int64(1234), off)
	assert.Empty(t, r.Markets())
}

func TestRestoreRejectsCorruption(t *testing.T) {
	p := partition.New()
	run(p, genStream(3, 200))
	data := p.Checkpoint(200, 6)
	for i := 0; i < len(data); i += 11 {
		bad := append([]byte(nil), data...)
		bad[i] ^= 0xff
		_, _, _, err := partition.Restore(bad)
		assert.Error(t, err, "flip byte %d", i)
	}
	for _, n := range []int{0, 3, 19, len(data) - 1} {
		_, _, _, err := partition.Restore(data[:n])
		assert.Error(t, err)
	}
}

// The wire enums must stay numerically identical to the engine constants
// because conversion is a plain cast.
func TestWireEnumsMirrorEngine(t *testing.T) {
	assert.EqualValues(t, engine.CmdPlace, matchingv1.CommandType_COMMAND_TYPE_PLACE)
	assert.EqualValues(t, engine.CmdCancel, matchingv1.CommandType_COMMAND_TYPE_CANCEL)
	assert.EqualValues(t, engine.Limit, matchingv1.OrderType_ORDER_TYPE_LIMIT)
	assert.EqualValues(t, engine.Market, matchingv1.OrderType_ORDER_TYPE_MARKET)
	assert.EqualValues(t, engine.GTC, matchingv1.TimeInForce_TIME_IN_FORCE_GTC)
	assert.EqualValues(t, engine.IOC, matchingv1.TimeInForce_TIME_IN_FORCE_IOC)
	assert.EqualValues(t, engine.FOK, matchingv1.TimeInForce_TIME_IN_FORCE_FOK)
	assert.EqualValues(t, engine.SideBuy, commonv1.Side_SIDE_BUY)
	assert.EqualValues(t, engine.SideSell, commonv1.Side_SIDE_SELL)
	for et, pt := range map[engine.EventType]matchingv1.EventType{
		engine.EvAccepted: matchingv1.EventType_EVENT_TYPE_ACCEPTED, engine.EvRejected: matchingv1.EventType_EVENT_TYPE_REJECTED,
		engine.EvTrade: matchingv1.EventType_EVENT_TYPE_TRADE, engine.EvCancelled: matchingv1.EventType_EVENT_TYPE_CANCELLED,
		engine.EvBookDelta: matchingv1.EventType_EVENT_TYPE_BOOK_DELTA,
	} {
		assert.EqualValues(t, et, pt)
	}
	for er, pr := range map[engine.Reason]matchingv1.Reason{
		engine.RejInvalidQty: matchingv1.Reason_REASON_INVALID_QTY, engine.RejInvalidPrice: matchingv1.Reason_REASON_INVALID_PRICE,
		engine.RejInvalidSide: matchingv1.Reason_REASON_INVALID_SIDE, engine.RejInvalidCombo: matchingv1.Reason_REASON_INVALID_COMBO,
		engine.RejDuplicateOrder: matchingv1.Reason_REASON_DUPLICATE_ORDER, engine.RejPostOnlyWouldCross: matchingv1.Reason_REASON_POST_ONLY_WOULD_CROSS,
		engine.RejUnknownOrder: matchingv1.Reason_REASON_UNKNOWN_ORDER, engine.RejUnknownCommand: matchingv1.Reason_REASON_UNKNOWN_COMMAND,
		engine.CancelUser: matchingv1.Reason_REASON_CANCEL_USER, engine.CancelIOCRemainder: matchingv1.Reason_REASON_CANCEL_IOC_REMAINDER,
		engine.CancelFOKUnfilled: matchingv1.Reason_REASON_CANCEL_FOK_UNFILLED, engine.CancelSelfTrade: matchingv1.Reason_REASON_CANCEL_SELF_TRADE,
		engine.RejInvalidOrderID: matchingv1.Reason_REASON_INVALID_ORDER_ID,
	} {
		assert.EqualValues(t, er, pr)
	}
}

func TestToProtoCopiesAllFields(t *testing.T) {
	ev := engine.Event{Seq: 9, Time: 8, Type: engine.EvTrade, Side: engine.SideSell, OrderID: 1, UserID: 2,
		MakerOrderID: 3, MakerUserID: 4, Price: 5, Qty: 6, TakerLeaves: 7, MakerLeaves: 10, TradeID: 11}
	p := partition.ToProto("M", ev)
	assert.Equal(t, &matchingv1.Event{Market: "M", Seq: 9, TimeUnixNano: 8, Type: matchingv1.EventType_EVENT_TYPE_TRADE,
		Side: commonv1.Side_SIDE_SELL, OrderId: 1, UserId: 2, MakerOrderId: 3, MakerUserId: 4, Price: 5, Qty: 6,
		TakerLeaves: 7, MakerLeaves: 10, TradeId: 11}, p)
}

func TestRestoreNamesTheOldCheckpointFormat(t *testing.T) {
	old := append([]byte("TLCK"), make([]byte, 20)...)
	_, _, _, err := partition.Restore(old)
	require.ErrorIs(t, err, partition.ErrCorruptCheckpoint)
	assert.Contains(t, err.Error(), "before Phase 3.1")
}
