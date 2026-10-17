package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/matching/internal/partition"
)

func writeCkpt(t *testing.T, dir string, offset int64, topicPartitions int) {
	t.Helper()
	p := partition.New()
	name := filepath.Join(dir, filepathName(offset))
	require.NoError(t, os.WriteFile(name, p.Checkpoint(offset, topicPartitions), 0o600))
}

func filepathName(offset int64) string { return filepath.Base(ckptName(offset)) }

func TestLoadCheckpointEmptyDirIsAFreshStart(t *testing.T) {
	p, next, err := loadCheckpoint(t.TempDir(), 6)
	require.NoError(t, err)
	assert.Zero(t, next)
	assert.Empty(t, p.Markets())
}

func TestLoadCheckpointRefusesWhenNoFileRestores(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ckptName(10)), []byte("garbage"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ckptName(20)), []byte("TLC2"), 0o600))
	_, _, err := loadCheckpoint(dir, 6)
	require.ErrorIs(t, err, ErrRefuseToStart)
	assert.Contains(t, err.Error(), "none can be restored")
}

func TestLoadCheckpointFallsBackToOlderValidFile(t *testing.T) {
	dir := t.TempDir()
	writeCkpt(t, dir, 100, 6)
	require.NoError(t, os.WriteFile(filepath.Join(dir, ckptName(200)), []byte("torn write"), 0o600))
	_, next, err := loadCheckpoint(dir, 6)
	require.NoError(t, err)
	assert.EqualValues(t, 100, next, "a torn newest file falls back to the previous valid checkpoint")
}

func TestLoadCheckpointRefusesChangedPartitionCount(t *testing.T) {
	dir := t.TempDir()
	writeCkpt(t, dir, 100, 6)
	_, _, err := loadCheckpoint(dir, 12)
	require.ErrorIs(t, err, ErrRefuseToStart)
	assert.Contains(t, err.Error(), "6 partitions")
	assert.Contains(t, err.Error(), "now has 12")
}
