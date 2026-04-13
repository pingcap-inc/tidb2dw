package snowsql

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

type SnowflakeRowApplier struct {
	storage         storage.ExternalStorage
	initSchemaFn    func([]cloudstorage.TableCol) error
	execDDLFn       func(cloudstorage.TableDefinition) error
	createTableFn   func(cloudstorage.TableDefinition) error
	loadIncrementFn func(cloudstorage.TableDefinition, string) error
	closeFn         func()
}

func NewSnowflakeRowApplier(
	sfConfig *SnowflakeConfig,
	stageName string,
	storageURI *url.URL,
	credentials *credentials.Value,
) (*SnowflakeRowApplier, error) {
	connector, err := NewSnowflakeConnector(sfConfig, stageName, storageURI, credentials)
	if err != nil {
		return nil, err
	}

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(context.Background(), storageURI.String())
	if err != nil {
		connector.Close()
		return nil, err
	}

	return &SnowflakeRowApplier{
		storage:      extStorage,
		initSchemaFn: connector.InitSchema,
		execDDLFn:    connector.ExecDDL,
		createTableFn: func(tableDef cloudstorage.TableDefinition) error {
			createTableQuery, err := GenCreateSchemaFromDefinition(tableDef)
			if err != nil {
				return err
			}
			if _, err := connector.db.Exec(createTableQuery); err != nil {
				return err
			}
			connector.columns = tableDef.Columns
			return nil
		},
		loadIncrementFn: connector.LoadIncrement,
		closeFn:         connector.Close,
	}, nil
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
