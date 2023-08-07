package redshiftsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/export"
	"gitlab.com/tymonx/go-formatter/formatter"
	"golang.org/x/exp/slices"
)

func GetServerSideTimestamp(db *sql.DB) (string, error) {
	var result string
	err := db.QueryRow("SELECT GETDATE();").Scan(&result)
	if err != nil {
		return "", errors.Trace(err)
	}
	return result, nil
}

// redshift currently can not support ROWS_PRODUCED function
// use csv file path for stageUrl, like s3://tidbbucket/snapshot/stock.csv
func LoadSnapshotFromStage(db *sql.DB, targetTable, storageUrl, filePrefix string, credential *credentials.Value, onSnapshotLoadProgress func(loadedRows int64)) error {
	// TODO(lcui2): check SQL query format
	sql, err := formatter.Format(`
	COPY {targetTable}
	FROM '{stageName}/{filePrefix}'
	CREDENTIALS 'aws_access_key_id={accessId};aws_secret_access_key={accessKey}'
	FORMAT AS CSV DELIMITER ',' QUOTE '"';
	`, formatter.Named{
		"targetTable": snowsql.EscapeString(targetTable),
		"stageName":   snowsql.EscapeString(storageUrl),
		"filePrefix":  snowsql.EscapeString(filePrefix), // TODO: Verify
		"accessId":    credential.AccessKeyID,
		"accessKey":   credential.SecretAccessKey,
	})
	if err != nil {
		return errors.Trace(err)
	}

	ctx := context.Background()
	_, err = db.ExecContext(ctx, sql)

	return err
}

func GenDropSchema(sourceTable string) (string, error) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", sourceTable)
	return sql, nil
}

func GenCreateSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) (string, error) {
	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	columnRows := make([]string, 0, len(tableColumns))
	for _, column := range tableColumns {
		row, err := GetRedshiftTypeString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	indexQuery := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", sourceDatabase, sourceTable) // FIXME: Escape
	indexRows, err := sourceTiDBConn.QueryContext(context.Background(), indexQuery)
	if err != nil {
		return "", errors.Trace(err)
	}
	indexResults, err := export.GetSpecifiedColumnValuesAndClose(indexRows, "KEY_NAME", "COLUMN_NAME", "SEQ_IN_INDEX")
	if err != nil {
		return "", errors.Trace(err)
	}

	redshiftPKColumns := make([]string, 0)
	// Sort by key_name, seq_in_index
	slices.SortFunc(indexResults, func(i, j []string) bool {
		if i[0] == j[0] {
			return i[2] < j[2] // Sort by seq_in_index
		}
		return i[0] < j[0] // Sort by key_name
	})
	for _, oneRow := range indexResults {
		keyName, columnName := oneRow[0], oneRow[1]
		if keyName == "PRIMARY" {
			redshiftPKColumns = append(redshiftPKColumns, columnName)
		}
	}

	// TODO: Support unique key

	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	if len(redshiftPKColumns) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(redshiftPKColumns, ", ")))
	}
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
