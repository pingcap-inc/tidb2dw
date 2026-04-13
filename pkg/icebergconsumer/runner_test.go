package icebergconsumer

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

type fakeRowApplier struct {
	createdTables []cloudstorage.TableDefinition
	appliedRows   int
}

func (f *fakeRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	f.createdTables = append(f.createdTables, tableDef)
	return nil
}

func (f *fakeRowApplier) InitSchema(_ []cloudstorage.TableCol) error {
	return nil
}

func (f *fakeRowApplier) ExecDDL(_ cloudstorage.TableDefinition) error {
	return nil
}

func (f *fakeRowApplier) ApplyRows(_ cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	f.appliedRows += len(rows)
	return nil
}

func (f *fakeRowApplier) Close() {}

func TestRunIncrementalOnlyCreatesTableWithoutBackfill(t *testing.T) {
	tableDef := cloudstorage.TableDefinition{
		Schema: "test_schema",
		Table:  "test_table",
	}
	applier := &fakeRowApplier{}

	runner := NewRunner(
		&FakeSource{
			Tables: []VersionedTable{
				{
					TableDef:      tableDef,
					LatestVersion: 1,
				},
			},
		},
		map[string]coreinterfaces.RowApplier{
			"test_schema.test_table": applier,
		},
		ModeIncrementalOnly,
	)

	err := runner.RunOnce()
	require.NoError(t, err)
	require.Len(t, applier.createdTables, 1)
	require.Equal(t, tableDef, applier.createdTables[0])
	require.Zero(t, applier.appliedRows)
}

func TestRunIncrementalOnlySkipsTableCreationForUnchangedVersion(t *testing.T) {
	tableDef := cloudstorage.TableDefinition{
		Schema: "test_schema",
		Table:  "test_table",
	}
	applier := &fakeRowApplier{}
	runner := NewRunner(
		&FakeSource{
			Tables: []VersionedTable{
				{
					TableDef:      tableDef,
					LatestVersion: 7,
				},
			},
		},
		map[string]coreinterfaces.RowApplier{
			"test_schema.test_table": applier,
		},
		ModeIncrementalOnly,
	)

	err := runner.RunOnce()
	require.NoError(t, err)

	err = runner.RunOnce()
	require.NoError(t, err)

	require.Len(t, applier.createdTables, 1)
	require.Equal(t, tableDef, applier.createdTables[0])
	require.Zero(t, applier.appliedRows)
}
