package databrickssql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

type DatabricksConnector struct {
	db         *sql.DB
	ctx        context.Context
	storageURL string
	credential string
	columns    []cloudstorage.TableCol
}

const incrementTablePrefix = "incr_"

func NewDatabricksConnector(databricksDB *sql.DB, credential string, storageURI *url.URL) (*DatabricksConnector, error) {
	storageURL := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)

	credentialSet, err := GetCredentialNameSet(databricksDB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if credential != "" {
		if _, exist := credentialSet[credential]; !exist {
			return nil, errors.Errorf("credential name [%s] is not found in databricks", credential)
		}
	} else {
		if len(credentialSet) > 1 {
			return nil, errors.Errorf("multiple credential found in databricks, please specify one")
		}
		for credentialName := range credentialSet {
			credential = credentialName
		}
	}

	return &DatabricksConnector{
		db:         databricksDB,
		ctx:        context.Background(),
		credential: credential,
		storageURL: storageURL,
		columns:    nil,
	}, nil
}

func (dc *DatabricksConnector) InitSchema(columns []cloudstorage.TableCol) error {
	if len(dc.columns) != 0 {
		return nil
	}
	if len(columns) == 0 {
		return errors.New("Columns in schema is empty")
	}
	dc.columns = columns
	log.Info("table columns initialized", zap.Any("Columns", columns))
	return nil
}

func (dc *DatabricksConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	dropTableSQL := GenDropTableSQL(sourceTable)
	if _, err := dc.db.Exec(dropTableSQL); err != nil {
		return errors.Trace(err)
	}

	if err := dc.setColumns(sourceDatabase, sourceTable, sourceTiDBConn); err != nil {
		return errors.Trace(err)
	}

	createTableSQL, err := GenCreateTableSQL(sourceTable, dc.columns)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Databricks Warehouse", zap.String("query", createTableSQL))

	if _, err := dc.db.Exec(createTableSQL); err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

func (dc *DatabricksConnector) LoadSnapshot(targetTable, filePath string) error {
	if err := LoadCSVFromS3(dc.db, dc.columns, targetTable, dc.storageURL, filePath, dc.credential); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePath", filePath))
	return nil
}

func (dc *DatabricksConnector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if len(dc.columns) == 0 {
		return errors.New("Columns not initialized. Maybe you execute a DDL before all DMLs, which is not supported now.")
	}
	ddls, err := GenDDLViaColumnsDiff(dc.columns, tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("No need to execute this DDL in Databricks", zap.String("ddl", tableDef.Query))
		return nil
	}
	// One DDL may be rewritten to multiple DDLs
	for _, ddl := range ddls {
		_, err := dc.db.Exec(ddl)
		if err != nil {
			log.Error("Failed to executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
			return errors.Annotate(err, fmt.Sprint("failed to execute", ddl))
		}
	}
	// update columns
	dc.columns = tableDef.Columns
	log.Info("Successfully executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
	return nil
}

func (dc *DatabricksConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	absolutePath := fmt.Sprintf("%s://%s%s/%s", uri.Scheme, uri.Host, uri.Path, filePath)
	incrTableColumns := utils.GenIncrementTableColumns(tableDef.Columns)
	incrTableName := incrementTablePrefix + tableDef.Table

	createExtTableSQL, err := GenCreateExternalTableSQL(incrTableName, incrTableColumns, absolutePath, dc.credential)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = dc.db.Exec(createExtTableSQL)
	if err != nil {
		return errors.Trace(err)
	}

	// Merge and delete increase table
	mergeIntoSQL := GenMergeIntoSQL(tableDef, tableDef.Table, incrTableName)
	_, err = dc.db.Exec(mergeIntoSQL)
	if err != nil {
		return errors.Trace(err)
	}

	dropTableSQL := GenDropTableSQL(incrTableName)
	_, err = dc.db.Exec(dropTableSQL)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (dc *DatabricksConnector) Close() {
	dc.db.Close()
}

func (dc *DatabricksConnector) setColumns(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	if dc.columns != nil {
		return nil
	}

	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return errors.Trace(err)
	}
	dc.columns = tableColumns
	return nil
}
