package icebergconsumer

import (
	"fmt"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
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
	source                Source
	appliers              map[string]coreinterfaces.RowApplier
	mode                  Mode
	lastProcessedVersions map[string]int
}

func NewRunner(source Source, appliers map[string]coreinterfaces.RowApplier, mode Mode) *Runner {
	return &Runner{
		source:                source,
		appliers:              appliers,
		mode:                  mode,
		lastProcessedVersions: make(map[string]int),
	}
}

func (r *Runner) RunOnce() error {
	tables, err := r.source.ListTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		key := tableKey(table.TableDef)
		applier, ok := r.appliers[key]
		if !ok {
			continue
		}

		if r.mode == ModeIncrementalOnly {
			if lastVersion, seen := r.lastProcessedVersions[key]; seen && lastVersion >= table.LatestVersion {
				continue
			}
			if err := applier.CreateTableFromDefinition(table.TableDef); err != nil {
				return err
			}
			r.lastProcessedVersions[key] = table.LatestVersion
		}
	}

	return nil
}

func tableKey(tableDef cloudstorage.TableDefinition) string {
	return fmt.Sprintf("%s.%s", tableDef.Schema, tableDef.Table)
}
