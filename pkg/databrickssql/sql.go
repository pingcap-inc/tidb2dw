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
	"github.com/pingcap-inc/tidb2dw/pkg/bigquerysql"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
	"strings"
)

func GenMergeIntoSQL(tableDef cloudstorage.TableDefinition, tableName, externalTableName string) string {
	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
			onStat = append(onStat, fmt.Sprintf(`T.%s = S.%s`, col.Name, col.Name))
		}
	}

	updateStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		updateStat = append(updateStat, fmt.Sprintf(`%s = S.%s`, col.Name, col.Name))
	}

	insertStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		insertStat = append(insertStat, col.Name)
	}

	valuesStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S.%s`, col.Name))
	}

	mergeSQL := fmt.Sprintf(
		`MERGE INTO %s AS T USING
	(
		SELECT * EXCEPT(row_num)
		FROM (
			SELECT
				*, row_number() over (partition by %s order by %s desc) as row_num
			FROM %s
		)
		WHERE row_num = 1
	) AS S
	ON
	(
		%s
	)
	WHEN MATCHED AND S.%s != 'D' THEN UPDATE SET %s
	WHEN MATCHED AND S.%s = 'D' THEN DELETE
	WHEN NOT MATCHED AND S.%s != 'D' THEN INSERT (%s) VALUES (%s);`,
		fmt.Sprintf("`%s`", tableName),
		strings.Join(pkColumn, ", "),
		bigquerysql.CDC_COMMIT_TS_COLUMN_NAME,
		fmt.Sprintf("`%s`", externalTableName),
		strings.Join(onStat, " AND "),
		bigquerysql.CDC_FLAG_COLUMN_NAME,
		strings.Join(updateStat, ", "),
		bigquerysql.CDC_FLAG_COLUMN_NAME,
		bigquerysql.CDC_FLAG_COLUMN_NAME,
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "),
	)

	return mergeSQL
}

func GenDropTableSQL(sourceTable string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", sourceTable)
}

func GenCreateTableSQL(tableName string, tableColumns []cloudstorage.TableCol) (string, error) {
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
	sql = append(sql, fmt.Sprintf(`CREATE TABLE %s (`, tableName)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), nil
}

func LoadCSVFromS3(db *sql.DB, columns []cloudstorage.TableCol, targetTable, storageUri, filePrefix string, temporaryCredentials *credentials.Credentials) error {
	tempCredential, err := temporaryCredentials.Get()
	if err != nil {
		return errors.Trace(err)
	}

	columnCastAndRenameSQL, err := buildColumnCastAndRename(columns)
	if err != nil {
		return errors.Trace(err)
	}

	patternSQL := `PATTERN = '*{filePrefix}*.csv'`
	if filePrefix == "" {
		// Without prefix, we need to specify the file name
		patternSQL = ""
	}

	copyIntoSQL := fmt.Sprintf(`
	COPY INTO {targetTable}
	FROM (
		SELECT {castAndRenameColumns}
		FROM '{storageUrl}' WITH (
		  CREDENTIAL (AWS_ACCESS_KEY = '{accessId}', AWS_SECRET_KEY = '{accessKey}', AWS_SESSION_TOKEN = '{sessionToken}')
		)
	)
	FILEFORMAT = CSV
	%s
	FORMAT_OPTIONS ('delimiter' = ',', 'inferSchema' = 'true')
	COPY_OPTIONS ('mergeSchema' = 'true');
	`, patternSQL)

	sql, err := formatter.Format(copyIntoSQL, formatter.Named{
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
	log.Info("Loading CSV data from AWS s3", zap.String("query", sql))
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
