package snowsql

import (
	"errors"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

type SnowflakeRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *SnowflakeRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	if a.createTableFn == nil {
		return errors.New("snowflake createTableFn is not configured")
	}
	return a.createTableFn(tableDef)
}

func (a *SnowflakeRowApplier) InitSchema(columns []cloudstorage.TableCol) error {
	if a.initSchemaFn == nil {
		return errors.New("snowflake initSchemaFn is not configured")
	}
	return a.initSchemaFn(columns)
}

func (a *SnowflakeRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if a.execDDLFn == nil {
		return errors.New("snowflake execDDLFn is not configured")
	}
	return a.execDDLFn(tableDef)
}

func (a *SnowflakeRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	if a.loadIncrementFn == nil {
		return errors.New("snowflake loadIncrementFn is not configured")
	}

	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}

func (a *SnowflakeRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
