package icebergconsumer

import (
	"fmt"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

type VersionedTable struct {
	TableDef      cloudstorage.TableDefinition
	LatestVersion int
}

type Source interface {
	ListTables() ([]VersionedTable, error)
}

type FakeSource struct {
	Tables []VersionedTable
	Err    error
}

func (f *FakeSource) ListTables() ([]VersionedTable, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Tables, nil
}

type Runner struct {
	source   Source
	appliers map[string]coreinterfaces.RowApplier
	mode     Mode
}

func NewRunner(source Source, appliers map[string]coreinterfaces.RowApplier, mode Mode) *Runner {
	return &Runner{
		source:   source,
		appliers: appliers,
		mode:     mode,
	}
}

func (r *Runner) RunOnce() error {
	tables, err := r.source.ListTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		applier, ok := r.appliers[tableKey(table.TableDef)]
		if !ok {
			continue
		}

		if r.mode == ModeIncrementalOnly {
			if err := applier.CreateTableFromDefinition(table.TableDef); err != nil {
				return err
			}
		}
	}

	return nil
}

func tableKey(tableDef cloudstorage.TableDefinition) string {
	return fmt.Sprintf("%s.%s", tableDef.Schema, tableDef.Table)
}
