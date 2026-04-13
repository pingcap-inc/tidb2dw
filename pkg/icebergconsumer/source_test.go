package icebergconsumer

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/stretchr/testify/require"
)

func TestToRowChangesParsesCommitTs(t *testing.T) {
	name := "alice"
	rows := []sinkiceberg.ChangeRow{
		{
			Op:         "I",
			CommitTs:   "101",
			CommitTime: "2026-04-13 10:00:00",
			Columns: map[string]*string{
				"name": &name,
			},
		},
	}

	changes, err := ToRowChanges(rows)
	require.NoError(t, err)
	require.Equal(t, []coreinterfaces.RowChange{
		{
			Op:         "I",
			CommitTs:   101,
			CommitTime: "2026-04-13 10:00:00",
			Columns: map[string]*string{
				"name": &name,
			},
		},
	}, changes)
}
