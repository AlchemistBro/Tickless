//go:build integration

package service_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	usersv1 "github.com/tickless/pkg/gen/tickless/users/v1"
)

func decodeToken(t *testing.T, payload []byte) string {
	t.Helper()
	var ev usersv1.UserSignedUp
	require.NoError(t, proto.Unmarshal(payload, &ev))
	require.NotEmpty(t, ev.VerificationToken)
	return ev.VerificationToken
}
