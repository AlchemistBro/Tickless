package scan_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/chainwatcher/internal/scan/scantest"
	"github.com/tickless/pkg/money"
)

func transfer(tx string, idx int64, addr string) scan.Transfer {
	return scan.Transfer{TxID: tx, Index: idx, Address: addr, Asset: "BTC", Amount: money.FromInt64(5)}
}

func TestReorgDropsUnconfirmedDepositWithoutCredit(t *testing.T) {
	c := scantest.NewChain("c")
	st := scantest.NewMemStore("alice")
	s := &scan.Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 2}
	k := scantest.Key{TxID: "tx1"}

	c.Mine()
	c.Mine(transfer("tx1", 0, "alice"))
	scantest.Sync(t, s)
	require.Equal(t, "PENDING", st.Status(t, k))
	require.EqualValues(t, 1, st.Confirmations(k))

	c.Reorg(3, 0, false) // tx1 is not on the winning branch
	scantest.Sync(t, s)
	require.Equal(t, "REORGED", st.Status(t, k))

	for i := 0; i < 5; i++ {
		c.Mine()
	}
	scantest.Sync(t, s)
	require.Equal(t, "REORGED", st.Status(t, k))
	require.Zero(t, st.Credits(t)[k], "a reorged deposit must never be credited")
}

func TestRemindedDepositIsCreditedOnceInItsNewBlock(t *testing.T) {
	c := scantest.NewChain("c")
	st := scantest.NewMemStore("alice")
	s := &scan.Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 3}
	k := scantest.Key{TxID: "tx1", Index: 1}

	c.Mine(transfer("tx1", 1, "alice"))
	c.Mine()
	scantest.Sync(t, s)
	c.Reorg(2, 0, true) // same height, other hashes; the tx is mined again on the new branch
	scantest.Sync(t, s)
	require.Equal(t, "PENDING", st.Status(t, k))
	require.EqualValues(t, 1, st.Height(k), "re-mined in the first block of the new branch")

	c.Mine()
	scantest.Sync(t, s)
	require.Equal(t, "CONFIRMED", st.Status(t, k))
	require.Equal(t, map[scantest.Key]int{k: 1}, st.Credits(t))
}

func TestShorterChainIsDetectedAsReorg(t *testing.T) {
	c := scantest.NewChain("c")
	st := scantest.NewMemStore("bob")
	s := &scan.Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 6}
	for i := 0; i < 4; i++ {
		c.Mine()
	}
	c.Mine(transfer("tx9", 0, "bob"))
	scantest.Sync(t, s)
	c.Shorten(3) // the tip goes DOWN; there is nothing new to extend
	scantest.Sync(t, s)
	require.Equal(t, "REORGED", st.Status(t, scantest.Key{TxID: "tx9"}))
	head, _, _, err := st.Head(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, head)
}

func TestDeepReorgOfConfirmedDepositIsReportedNotHidden(t *testing.T) {
	c := scantest.NewChain("c")
	st := scantest.NewMemStore("alice")
	var lost int
	s := &scan.Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 2, OnDeepReorg: func(n int) { lost += n }}
	k := scantest.Key{TxID: "tx1"}
	c.Mine(transfer("tx1", 0, "alice"))
	c.Mine()
	c.Mine()
	scantest.Sync(t, s)
	require.Equal(t, "CONFIRMED", st.Status(t, k))

	c.Reorg(3, 1, false)
	scantest.Sync(t, s)
	require.Equal(t, 1, lost, "a reorg deeper than the threshold must be reported")
	require.Equal(t, 1, st.Credits(t)[k], "and never credited twice")
}

func TestRequiredConfirmationsMustBePositive(t *testing.T) {
	s := &scan.Scanner{Chain: scantest.NewChain("c"), Store: scantest.NewMemStore(), Name: "test", Required: 0}
	_, err := s.Step(context.Background())
	require.Error(t, err)
}

// SPEC §6 invariants 4 and 5 over 300 random histories (reference store).
func TestRandomChainHistoriesKeepInvariants4And5(t *testing.T) {
	scantest.RunRandomHistories(t, 300, 120, scantest.MemFactory)
}
