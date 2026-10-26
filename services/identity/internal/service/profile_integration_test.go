//go:build integration

package service_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/identity/internal/service"
)

func TestProfileReflectsTOTPAndNotificationPrefs(t *testing.T) {
	e, ctx := setup(t, true)
	id, err := e.svc.Signup(ctx, "frank@example.com", "correct horse battery")
	require.NoError(t, err)

	p, err := e.svc.GetProfile(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "frank@example.com", p.Email)
	require.True(t, p.Verified)
	require.False(t, p.TOTPEnabled)
	require.False(t, p.NotifyOrderFills, "order-filled emails are opt-in")

	secret, err := e.svc.EnrollTOTP(ctx, id)
	require.NoError(t, err)
	p, _ = e.svc.GetProfile(ctx, id)
	require.False(t, p.TOTPEnabled, "an unconfirmed secret does not enable 2FA")
	require.NoError(t, e.svc.ConfirmTOTP(ctx, id, codeAt(t, secret, e.now)))
	p, _ = e.svc.GetProfile(ctx, id)
	require.True(t, p.TOTPEnabled)

	require.NoError(t, e.svc.SetNotifyOrderFills(ctx, id, true))
	p, _ = e.svc.GetProfile(ctx, id)
	require.True(t, p.NotifyOrderFills)

	_, err = e.svc.GetProfile(ctx, 999999)
	require.ErrorIs(t, err, service.ErrNotFound)
	require.ErrorIs(t, e.svc.SetNotifyOrderFills(ctx, 999999, true), service.ErrNotFound)
}
