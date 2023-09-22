// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databrickssql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/bigquerysql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
	"net/url"
	"strings"
)

type (
	DatabricksConnector struct {
		db  *sql.DB
		ctx context.Context

		temporaryCredentials *credentials.Credentials

		storageURL string
		awsRegion  string

		columns []cloudstorage.TableCol
	}
)

const incrementTablePrefix = "incr_"

func NewDatabricksConnector(databricksDB *sql.DB, srcCredentials *credentials.Value,
	storageURI *url.URL, awsRegion string) (*DatabricksConnector, error) {
	storageURL := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)

	return &DatabricksConnector{
		db:  databricksDB,
		ctx: context.Background(),

		temporaryCredentials: credentials.NewCredentials(NewTemporaryCredentialsProvider(srcCredentials, awsRegion)),

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
	_, err := dc.db.Exec(dropTableSQL)
	if err != nil {
		return errors.Trace(err)
	}

	err = dc.setColumns(sourceDatabase, sourceTable, sourceTiDBConn)
	createTableSQL, err := GenCreateTableSQL(sourceTable, dc.columns)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Databricks Warehouse", zap.String("query", createTableSQL))

	_, err = dc.db.Exec(createTableSQL)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

func (dc *DatabricksConnector) LoadSnapshot(targetTable, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error {
	if err := LoadCSVFromS3(dc.db, dc.columns, targetTable, dc.storageURL, filePrefix, dc.temporaryCredentials); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePrefix", filePrefix))
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
	incrTableColumns := bigquerysql.GenIncrementTableColumns(tableDef.Columns)
	incrTableName := incrementTablePrefix + tableDef.Table

	createTableSQL, err := GenCreateTableSQL(incrTableName, incrTableColumns)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = dc.db.Exec(createTableSQL)
	if err != nil {
		return errors.Trace(err)
	}

	if err := LoadCSVFromS3(dc.db, incrTableColumns, incrTableName, absolutePath, "",
		dc.temporaryCredentials); err != nil {
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
