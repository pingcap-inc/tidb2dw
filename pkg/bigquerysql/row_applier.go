package bigquerysql

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

type BigQueryRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func NewBigQueryRowApplier(
	bqConfig *BigQueryConfig,
	incrementTableID, datasetID, tableID string,
	storageURI *url.URL,
) (*BigQueryRowApplier, error) {
	connector, err := NewBigQueryConnector(bqConfig, incrementTableID, datasetID, tableID, storageURI)
	if err != nil {
		return nil, err
	}

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(context.Background(), storageURI.String())
	if err != nil {
		connector.Close()
		return nil, err
	}

	return &BigQueryRowApplier{
		storage:      extStorage,
		initSchemaFn: connector.InitSchema,
		execDDLFn:    connector.ExecDDL,
		createTableFn: func(tableDef cloudstorage.TableDefinition) error {
			primaryKeys := make([]string, 0, len(tableDef.Columns))
			for _, column := range tableDef.Columns {
				if column.IsPK == "true" {
					primaryKeys = append(primaryKeys, column.Name)
				}
			}
			createTableSQL, err := GenCreateSchema(tableDef.Columns, primaryKeys, connector.datasetID, connector.tableID)
			if err != nil {
				return err
			}
			if err := runQuery(connector.ctx, connector.bqClient, createTableSQL); err != nil {
				return err
			}
			connector.columns = tableDef.Columns
			return nil
		},
		loadIncrementFn: connector.LoadIncrement,
		closeFn:         connector.Close,
	}, nil
}

func (a *BigQueryRowApplier) CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error {
	if a.createTableFn == nil {
		return errors.New("bigquery createTableFn is not configured")
	}
	return a.createTableFn(tableDef)
}

func (a *BigQueryRowApplier) InitSchema(columns []cloudstorage.TableCol) error {
	if a.initSchemaFn == nil {
		return errors.New("bigquery initSchemaFn is not configured")
	}
	return a.initSchemaFn(columns)
}

func (a *BigQueryRowApplier) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if a.execDDLFn == nil {
		return errors.New("bigquery execDDLFn is not configured")
	}
	return a.execDDLFn(tableDef)
}

func (a *BigQueryRowApplier) ApplyRows(tableDef cloudstorage.TableDefinition, rows []coreinterfaces.RowChange) error {
	if a.loadIncrementFn == nil {
		return errors.New("bigquery loadIncrementFn is not configured")
	}

	filePath, err := rowstage.WriteIncrementFile(a.storage, tableDef.Schema+"."+tableDef.Table, tableDef, rows)
	if err != nil {
		return err
	}
	return a.loadIncrementFn(tableDef, filePath)
}

func (a *BigQueryRowApplier) Close() {
	if a.closeFn != nil {
		a.closeFn()
	}
}
