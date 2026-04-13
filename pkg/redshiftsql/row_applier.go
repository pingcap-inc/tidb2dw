package redshiftsql

import (
	"errors"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

type RedshiftRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func (a *RedshiftRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	if a.createTableFn == nil {
		return errors.New("redshift createTableFn is not configured")
	}
	return a.createTableFn(tableDef)
}

func (a *RedshiftRowApplier) InitSchema(columns []cloudstorage.TableCol) error {
	if a.initSchemaFn == nil {
		return errors.New("redshift initSchemaFn is not configured")
	}
	return a.initSchemaFn(columns)
}

func (a *RedshiftRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if a.execDDLFn == nil {
		return errors.New("redshift execDDLFn is not configured")
	}
	return a.execDDLFn(tableDef)
}

func (a *RedshiftRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	if a.loadIncrementFn == nil {
		return errors.New("redshift loadIncrementFn is not configured")
	}

	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}

func (a *RedshiftRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
