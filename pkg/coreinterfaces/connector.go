package coreinterfaces

import (
	"database/sql"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

/// Connector is the interface for Data Warehouse connector
/// One Connector is responsible for one table.
/// Any data warehouse should implement this interface.
/// All Data Warehouse related operations should be done through this.

type Connector interface {
	// InitSchema initializes the schema of the table
	InitSchema(columns []cloudstorage.TableCol) error
	// CopyTableSchema copies the table schema from the source database to the Data Warehouse
	CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error
	// LoadSnapshot loads the snapshot into the Data Warehouse
	LoadSnapshot(targetTable, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error
	// ExecDDL executes the DDL statements in Data Warehouse
	ExecDDL(tableDef cloudstorage.TableDefinition) error
	// LoadIncrement loads the increment data into the Data Warehouse
	LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error
	// Close closes the connection to the Data Warehouse
	Close()
}
