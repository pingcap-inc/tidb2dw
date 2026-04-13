package coreinterfaces

import "github.com/pingcap/tiflow/pkg/sink/cloudstorage"

type RowChange struct {
	Op         string
	CommitTs   uint64
	CommitTime string
	Columns    map[string]*string
}

type RowApplier interface {
	CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error
	InitSchema(columns []cloudstorage.TableCol) error
	ExecDDL(tableDef cloudstorage.TableDefinition) error
	ApplyRows(tableDef cloudstorage.TableDefinition, rows []RowChange) error
	Close()
}
