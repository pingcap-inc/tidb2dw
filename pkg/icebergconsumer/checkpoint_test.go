package icebergconsumer

import (
	"context"
	"net/url"
	"testing"
	"time"

	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	require.NoError(t, err)

	expected := Checkpoint{
		SourceID:            "source-1",
		Schema:              "test-schema",
		Table:               "test-table",
		LastMetadataVersion: 7,
		DDLApplied:          true,
		LastDataFile:        "data-0007.parquet",
	}

	err = SaveCheckpoint(extStorage, expected)
	require.NoError(t, err)

	actual, ok, err := LoadCheckpoint(extStorage, expected.SourceID, expected.Schema, expected.Table)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, actual)
}

func TestCheckpointSourceIsolation(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	require.NoError(t, err)

	first := Checkpoint{
		SourceID:            "source-1",
		Schema:              "shared-schema",
		Table:               "shared-table",
		LastMetadataVersion: 7,
		DDLApplied:          true,
		LastDataFile:        "source-1.parquet",
	}
	second := Checkpoint{
		SourceID:            "source-2",
		Schema:              first.Schema,
		Table:               first.Table,
		LastMetadataVersion: 9,
		DDLApplied:          false,
		LastDataFile:        "source-2.parquet",
	}

	require.NoError(t, SaveCheckpoint(extStorage, first))
	require.NoError(t, SaveCheckpoint(extStorage, second))

	actualFirst, ok, err := LoadCheckpoint(extStorage, first.SourceID, first.Schema, first.Table)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first, actualFirst)

	actualSecond, ok, err := LoadCheckpoint(extStorage, second.SourceID, second.Schema, second.Table)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, second, actualSecond)
}

func TestLoadCheckpointMissing(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
	require.NoError(t, err)

	checkpoint, ok, err := LoadCheckpoint(extStorage, "missing-source", "missing-schema", "missing-table")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, Checkpoint{}, checkpoint)
}

func TestNewConfigRejectsEmptySourceURI(t *testing.T) {
	cfg, err := NewConfig("", time.Second)
	require.Nil(t, cfg)
	require.ErrorContains(t, err, "iceberg source uri")
}

func TestNewConfigRejectsWhitespaceSourceURI(t *testing.T) {
	cfg, err := NewConfig("   \t\n  ", time.Second)
	require.Nil(t, cfg)
	require.ErrorContains(t, err, "iceberg source uri")
}

func TestNewConfigRejectsSourceURIWithoutScheme(t *testing.T) {
	cfg, err := NewConfig("not a uri", time.Second)
	require.Nil(t, cfg)
	require.ErrorContains(t, err, "iceberg source uri")
}

func TestNewConfigDefaultsPollInterval(t *testing.T) {
	cfg, err := NewConfig("file:///tmp/warehouse", 0)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, cfg.PollInterval)
	require.Equal(t, "file", cfg.SourceURI.Scheme)
	require.Equal(t, "/tmp/warehouse", cfg.SourceURI.Path)
}
