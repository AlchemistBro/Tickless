package rules

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDenylistIgnoresCaseCommentsAndBlankLines(t *testing.T) {
	p := filepath.Join(t.TempDir(), "deny.txt")
	require.NoError(t, os.WriteFile(p, []byte("# sanctioned\n\n0xAbCdEf0123456789aBcDeF0123456789AbCdEf01  # mixer\nbcrt1qbad\n"), 0o600))
	d, err := LoadDenylist(p)
	require.NoError(t, err)
	require.Equal(t, 2, d.Len())
	require.True(t, d.Listed("0xabcdef0123456789abcdef0123456789abcdef01"))
	require.True(t, d.Listed(" BCRT1QBAD "))
	require.False(t, d.Listed("bcrt1qgood"))

	require.NoError(t, os.WriteFile(p, []byte("bcrt1qgood\n"), 0o600))
	require.NoError(t, d.Reload())
	require.True(t, d.Listed("bcrt1qgood"))
	require.False(t, d.Listed("bcrt1qbad"), "reload replaces the list")

	empty, err := LoadDenylist("")
	require.NoError(t, err)
	require.False(t, empty.Listed("anything"))
	_, err = LoadDenylist(filepath.Join(t.TempDir(), "missing.txt"))
	require.Error(t, err, "a configured but missing list is an error, not an empty list")
}
