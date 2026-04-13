package icebergconsumer

import (
	"context"
	"net/url"
	"testing"
	"time"

	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
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

	actual, ok, err := LoadCheckpoint(extStorage, expected.Schema, expected.Table)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, actual)
}

func TestLoadCheckpointMissing(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	require.NoError(t, err)

	checkpoint, ok, err := LoadCheckpoint(extStorage, "missing-schema", "missing-table")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, Checkpoint{}, checkpoint)
}

func TestNewConfigRejectsEmptySourceURI(t *testing.T) {
	cfg, err := NewConfig("", time.Second)
	require.Nil(t, cfg)
	require.ErrorContains(t, err, "iceberg source uri")
}
