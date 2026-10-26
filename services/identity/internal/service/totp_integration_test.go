//go:build integration

package service_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tickless/identity/internal/service"
	"github.com/tickless/pkg/totp"
)

// codeAt computes the code an authenticator app shows at `at`, from the
// base32 secret returned by EnrollTOTP.
func codeAt(t *testing.T, enrolled string, at time.Time) string {
	t.Helper()
	raw, err := totpDecode(enrolled)
	require.NoError(t, err)
	return totp.Code(raw, totp.Step(at))
}

func TestTOTPEnrolConfirmAndVerifyOnce(t *testing.T) {
	e, ctx := setup(t, true)
	id, err := e.svc.Signup(ctx, "carol@example.com", "correct horse battery")
	require.NoError(t, err)

	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, "000000"), service.ErrTOTPNotSetUp, "nothing enrolled yet")

	secret, err := e.svc.EnrollTOTP(ctx, id)
	require.NoError(t, err)
	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, e.now)), service.ErrTOTPNotSetUp, "unconfirmed secrets never verify")

	require.ErrorIs(t, e.svc.ConfirmTOTP(ctx, id, "000000"), service.ErrTOTPInvalid)
	require.NoError(t, e.svc.ConfirmTOTP(ctx, id, codeAt(t, secret, e.now)))
	require.ErrorIs(t, e.svc.ConfirmTOTP(ctx, id, codeAt(t, secret, e.now)), service.ErrTOTPConfirmed)
	_, err = e.svc.EnrollTOTP(ctx, id)
	require.ErrorIs(t, err, service.ErrTOTPConfirmed, "a confirmed secret cannot be silently replaced")

	// The confirmation consumed the current step: its code is now a replay.
	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, e.now)), service.ErrTOTPInvalid, "the confirming code cannot be reused")
	// The next step's code is accepted once; the same code again is a replay.
	next := e.now.Add(totp.Period)
	require.NoError(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, next)))
	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, next)), service.ErrTOTPInvalid, "replay refused")
}

func TestTOTPDriftAndStepOrdering(t *testing.T) {
	e, ctx := setup(t, true)
	id, err := e.svc.Signup(ctx, "dave@example.com", "correct horse battery")
	require.NoError(t, err)
	secret, err := e.svc.EnrollTOTP(ctx, id)
	require.NoError(t, err)
	require.NoError(t, e.svc.ConfirmTOTP(ctx, id, codeAt(t, secret, e.now)))

	// The code of the next step is accepted within the drift window...
	later := e.now.Add(totp.Period)
	require.NoError(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, later)))
	// ...but after that step was used, the previous step's code is refused.
	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, e.now)), service.ErrTOTPInvalid, "an older step after a newer one")

	// Two steps away is outside the window.
	far := e.now.Add(10 * totp.Period)
	require.ErrorIs(t, e.svc.VerifyTOTP(ctx, id, codeAt(t, secret, far.Add(-5*totp.Period))), service.ErrTOTPInvalid)
}

// Concurrent verifications of the same code: exactly one succeeds.
func TestConcurrentVerifyOfOneCodeHasOneWinner(t *testing.T) {
	e, ctx := setup(t, true)
	id, err := e.svc.Signup(ctx, "erin@example.com", "correct horse battery")
	require.NoError(t, err)
	secret, err := e.svc.EnrollTOTP(ctx, id)
	require.NoError(t, err)
	require.NoError(t, e.svc.ConfirmTOTP(ctx, id, codeAt(t, secret, e.now)))

	later := e.now.Add(totp.Period)
	code := codeAt(t, secret, later)
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- e.svc.VerifyTOTP(ctx, id, code)
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, service.ErrTOTPInvalid)
		}
	}
	require.Equal(t, 1, wins, "one code, one use")
}
