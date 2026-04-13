package redshiftsql

import (
	"context"
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/rowstage"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type RedshiftRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func NewRedshiftRowApplier(
	rsConfig *RedshiftConfig,
	increTableName string,
	storageURI *url.URL,
	s3Credentials *credentials.Value,
) (*RedshiftRowApplier, error) {
	connector, err := NewRedshiftConnector(rsConfig, increTableName, storageURI, s3Credentials)
	if err != nil {
		return nil, err
	}

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(context.Background(), storageURI.String())
	if err != nil {
		connector.Close()
		return nil, err
	}

	return &RedshiftRowApplier{
		storage:      extStorage,
		initSchemaFn: connector.InitSchema,
		execDDLFn:    connector.ExecDDL,
		createTableFn: func(tableDef cloudstorage.TableDefinition) error {
			if err := DropTableIfExists(connector.db, tableDef.Table); err != nil {
				return err
			}
			createTableSQL, err := GenCreateTableSQL(tableDef)
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
