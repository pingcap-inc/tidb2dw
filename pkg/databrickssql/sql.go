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
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
	"strings"
)

func GenDropTableSQL(sourceTable string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", sourceTable)
}

func GenCreateTableSQL(sourceTable string, tableColumns []cloudstorage.TableCol) (string, error) {
	columnRows := make([]string, 0, len(tableColumns))
	for _, column := range tableColumns {
		row, err := GetDatabricksColumnString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	// TODO: Support unique key

	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE TABLE %s (`, sourceTable)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), nil
}

func LoadSnapshotFromS3(db *sql.DB, columns []cloudstorage.TableCol, targetTable, storageUri, filePrefix string, temporaryCredentials *credentials.Credentials) error {
	tempCredential, err := temporaryCredentials.Get()
	if err != nil {
		return errors.Trace(err)
	}

	columnCastAndRenameSQL, err := buildColumnCastAndRename(columns)
	if err != nil {
		return errors.Trace(err)
	}

	sql, err := formatter.Format(`
	COPY INTO {targetTable}
	FROM (
		SELECT {castAndRenameColumns}
		FROM '{storageUrl}' WITH (
		  CREDENTIAL (AWS_ACCESS_KEY = '{accessId}', AWS_SECRET_KEY = '{accessKey}', AWS_SESSION_TOKEN = '{sessionToken}')
		)
	)
	FILEFORMAT = CSV
	PATTERN = '*{filePrefix}*.csv'
	FORMAT_OPTIONS ('delimiter' = ',', 'inferSchema' = 'true')
	COPY_OPTIONS ('mergeSchema' = 'true');
	`, formatter.Named{
		"targetTable":          snowsql.EscapeString(targetTable),
		"castAndRenameColumns": columnCastAndRenameSQL,
		"storageUrl":           snowsql.EscapeString(storageUri),
		"filePrefix":           snowsql.EscapeString(filePrefix), // glob pattern // TODO: Verify
		"accessId":             tempCredential.AccessKeyID,
		"accessKey":            tempCredential.SecretAccessKey,
		"sessionToken":         tempCredential.SessionToken,
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Loading snapshot data from external table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

// buildColumnCastAndRename spark will generate field names as _c0, _c1, _c2, etc. for CSV files without header.
// Tested 512 columns, the pattern is _c{index} where index starts from 0
// refer to: https://stackoverflow.com/questions/75459116/databricks-sql-api-load-csv-file-without-header
func buildColumnCastAndRename(columns []cloudstorage.TableCol) (string, error) {
	wholeCastPartSQL := make([]string, 0, len(columns))
	for index, column := range columns {
		castType, err := GetDatabricksTypeString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		wholeCastPartSQL = append(wholeCastPartSQL, fmt.Sprintf("cast(_c%d as %s) as %s", index, castType, column.Name))
	}

	return strings.Join(wholeCastPartSQL, ", "), nil
}
