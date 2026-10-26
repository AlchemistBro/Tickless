//go:build integration

package app_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	walletv1 "github.com/tickless/pkg/gen/tickless/wallet/v1"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
	"github.com/tickless/wallet/app"
	"github.com/tickless/wallet/internal/store"
	"github.com/tickless/wallet/migrations"
)

// registrar records registrations and can be told to fail.
type registrar struct {
	mu    sync.Mutex
	fail  bool
	calls []*chainwatcherv1.RegisterAddressRequest
}

func (r *registrar) RegisterAddress(_ context.Context, in *chainwatcherv1.RegisterAddressRequest, _ ...grpc.CallOption) (*chainwatcherv1.RegisterAddressResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return nil, errors.New("chainwatcher down")
	}
	r.calls = append(r.calls, in)
	return &chainwatcherv1.RegisterAddressResponse{}, nil
}

func xpub(t *testing.T, purpose, coin uint32, p *chaincfg.Params) string {
	t.Helper()
	seed, err := hdkeychain.GenerateSeed(hdkeychain.RecommendedSeedLen)
	require.NoError(t, err)
	k, err := hdkeychain.NewMaster(seed, p)
	require.NoError(t, err)
	for _, i := range []uint32{purpose, coin, 0} {
		k, err = k.Derive(hdkeychain.HardenedKeyStart + i)
		require.NoError(t, err)
	}
	pub, err := k.Neuter()
	require.NoError(t, err)
	return pub.String()
}

func newServer(t *testing.T) (*app.Server, *registrar, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 40)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	ds, err := app.Derivers(app.Config{BTCXPub: xpub(t, 84, 1, &chaincfg.RegressionNetParams), ETHXPub: xpub(t, 44, 60, &chaincfg.MainNetParams), BTCNetwork: "regtest"})
	require.NoError(t, err)
	reg := &registrar{}
	return &app.Server{Store: store.New(pool), Derivers: ds, Registrar: reg}, reg, ctx
}

func TestConcurrentFirstCallsAgreeOnOneAddressPerUserAndChain(t *testing.T) {
	s, reg, ctx := newServer(t)
	const users, callsPerUser = 10, 8
	got := make([][]string, users)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for u := 0; u < users; u++ {
		for c := 0; c < callsPerUser; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: int64(u + 1), Chain: "btc"})
				require.NoError(t, err)
				mu.Lock()
				got[u] = append(got[u], r.Address)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	seen := map[string]int{}
	for u, addrs := range got {
		for _, a := range addrs {
			require.Equal(t, addrs[0], a, "user %d got different addresses", u+1)
		}
		seen[addrs[0]]++
	}
	require.Len(t, seen, users, "every user has a distinct address")

	owner := map[string]int64{}
	for _, c := range reg.calls {
		if prev, ok := owner[c.Address]; ok {
			require.Equal(t, prev, c.UserId, "an address is only ever registered to its owner")
		}
		owner[c.Address] = c.UserId
		require.Equal(t, "btc", c.Chain)
	}
	require.Len(t, owner, users)
}

func TestAddressIsReturnedOnlyAfterRegistrationAndSurvivesAFailure(t *testing.T) {
	s, reg, ctx := newServer(t)
	reg.fail = true
	_, err := s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: 7, Chain: "eth"})
	require.Equal(t, codes.Unavailable, status.Code(err), "no address is handed out unwatched")

	reg.fail = false
	r1, err := s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: 7, Chain: "eth"})
	require.NoError(t, err)
	require.Regexp(t, `^0x[0-9a-fA-F]{40}$`, r1.Address)
	require.Len(t, reg.calls, 1)
	require.Equal(t, r1.Address, reg.calls[0].Address)
	require.Equal(t, r1.DerivationIndex, reg.calls[0].DerivationIndex)

	r2, err := s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: 7, Chain: "eth"})
	require.NoError(t, err)
	require.Equal(t, r1.Address, r2.Address, "the address created before the failure is kept")
	require.Len(t, reg.calls, 1, "a registered address is not registered again")

	// The stored address is exactly the derivation at its index.
	want, err := s.Derivers["eth"].Address(r1.DerivationIndex)
	require.NoError(t, err)
	require.Equal(t, want, r1.Address)
}

func TestRejectsUnknownChainAndMainnet(t *testing.T) {
	s, _, ctx := newServer(t)
	_, err := s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: 1, Chain: "doge"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = s.GetDepositAddress(ctx, &walletv1.GetDepositAddressRequest{UserId: 0, Chain: "btc"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = app.Derivers(app.Config{BTCXPub: xpub(t, 84, 0, &chaincfg.MainNetParams), BTCNetwork: "mainnet"})
	require.ErrorContains(t, err, "regtest or testnet only")
}
