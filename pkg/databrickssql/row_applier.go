package databrickssql

import (
	"context"
	"errors"
	"net/url"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type DatabricksRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func NewDatabricksRowApplier(
	dbConfig *DataBricksConfig,
	credential string,
	storageURI *url.URL,
) (*DatabricksRowApplier, error) {
	connector, err := NewDatabricksConnector(dbConfig, credential, storageURI)
	if err != nil {
		return nil, err
	}

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(context.Background(), storageURI.String())
	if err != nil {
		connector.Close()
		return nil, err
	}

	return &DatabricksRowApplier{
		storage:      extStorage,
		initSchemaFn: connector.InitSchema,
		execDDLFn:    connector.ExecDDL,
		createTableFn: func(tableDef cloudstorage.TableDefinition) error {
			dropTableSQL := GenDropTableSQL(tableDef.Table)
			if _, err := connector.db.Exec(dropTableSQL); err != nil {
				return err
			}
			createTableSQL, err := GenCreateTableSQL(tableDef.Table, tableDef.Columns)
			if err != nil {
				return err
			}
			if _, err := connector.db.Exec(createTableSQL); err != nil {
				return err
			}
			connector.columns = tableDef.Columns
			return nil
		},
		loadIncrementFn: connector.LoadIncrement,
		closeFn:         connector.Close,
	}, nil
}

func (a *DatabricksRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	if a.createTableFn == nil {
		return errors.New("databricks createTableFn is not configured")
	}
	return a.createTableFn(tableDef)
}

func (a *DatabricksRowApplier) InitSchema(columns []cloudstorage.TableCol) error {
	if a.initSchemaFn == nil {
		return errors.New("databricks initSchemaFn is not configured")
	}
	return a.initSchemaFn(columns)
}

func (a *DatabricksRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if a.execDDLFn == nil {
		return errors.New("databricks execDDLFn is not configured")
	}
	return a.execDDLFn(tableDef)
}

func (a *DatabricksRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	if a.loadIncrementFn == nil {
		return errors.New("databricks loadIncrementFn is not configured")
	}

	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}

func (a *DatabricksRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
