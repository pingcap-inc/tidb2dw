package snowsql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/snowflakedb/gosnowflake"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

func CreateExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred *credentials.Value) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE {stageName}
URL = '{url}'
CREDENTIALS = (AWS_KEY_ID = '{awsKeyId}' AWS_SECRET_KEY = '{awsSecretKey}' AWS_TOKEN = '{awsToken}')
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
	`, formatter.Named{
		"stageName":    EscapeString(stageName),
		"url":          EscapeString(s3WorkspaceURL),
		"awsKeyId":     EscapeString(cred.AccessKeyID),
		"awsSecretKey": EscapeString(cred.SecretAccessKey),
		"awsToken":     EscapeString(cred.SessionToken),
	})
	if err != nil {
		return err
	}
	_, err = db.Exec(sql)
	return err
}

func CreateInternalStage(db *sql.DB, stageName string) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE {stageName}
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
`, formatter.Named{
		"stageName": EscapeString(stageName),
	})
	if err != nil {
		return err
	}
	_, err = db.Exec(sql)
	return err
}

func DropStage(db *sql.DB, stageName string) error {
	sql, err := formatter.Format(`
DROP STAGE IF EXISTS {stageName};
`, formatter.Named{
		"stageName": EscapeString(stageName),
	})
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(sql)
	return err
}

func GetServerSideTimestamp(db *sql.DB) (string, error) {
	var result string
	err := db.QueryRow("SELECT CURRENT_TIMESTAMP").Scan(&result)
	if err != nil {
		return "", errors.Trace(err)
	}
	return result, nil
}

func LoadSnapshotFromStage(db *sql.DB, targetTable, stageName, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error {
	// The timestamp and reqId is used to monitor the progress of COPY INTO query.
	ts, err := GetServerSideTimestamp(db)
	if err != nil {
		return errors.Trace(err)
	}
	reqId := gosnowflake.NewUUID()

	sql, err := formatter.Format(`
COPY INTO {targetTable}
-- tidb2dw-reqid={reqId}
FROM @{stageName}
FILE_FORMAT = (TYPE = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"')
PATTERN = '{filePrefix}.*'
ON_ERROR = CONTINUE;
`, formatter.Named{
		"reqId":       EscapeString(reqId.String()),
		"targetTable": EscapeString(targetTable),
		"stageName":   EscapeString(stageName),
		"filePrefix":  EscapeString(regexp.QuoteMeta(filePrefix)), // TODO: Verify
	})
	if err != nil {
		return errors.Trace(err)
	}

	ctx := gosnowflake.WithRequestID(context.Background(), reqId)

	wg := sync.WaitGroup{}
	wg.Add(1)

	copyFinished := make(chan struct{})

	go func() {
		// This is a goroutine to monitor the COPY INTO progress.
		defer wg.Done()

		if onSnapshotLoadProgress == nil {
			return
		}

		var rowsProduced int64

		checkInterval := 10 * time.Second
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-copyFinished:
				return
			case <-ticker.C:

				err := db.QueryRow(`
		SELECT ROWS_PRODUCED
		FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
				END_TIME_RANGE_START => ?::TIMESTAMP_LTZ,
				RESULT_LIMIT => 10000
		))
		WHERE QUERY_TYPE = 'COPY'
		AND CONTAINS(QUERY_TEXT, ?);
		`, ts, fmt.Sprintf("tidb2dw-reqid=%s", reqId.String())).Scan(&rowsProduced)
				if err != nil {
					log.Warn("Failed to get progress", zap.Error(err))
				}

				onSnapshotLoadProgress(rowsProduced)
			}
		}
	}()

	_, err = db.ExecContext(ctx, sql)
	copyFinished <- struct{}{}

	wg.Wait()

	return err
}

func GenCreateSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) (string, []cloudstorage.TableCol, error) {
	columnQuery := fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, COLUMN_KEY
FROM information_schema.columns
WHERE table_schema = "%s" AND table_name = "%s"`, sourceDatabase, sourceTable) // FIXME: Escape
	rows, err := sourceTiDBConn.QueryContext(context.Background(), columnQuery)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	// TODO: Confirm with generated column, sequence.
	defer rows.Close()
	columnRows := make([]string, 0)
	tableCol := make([]cloudstorage.TableCol, 0)
	for rows.Next() {
		var column struct {
			ColumnName    string
			ColumnDefault *string
			IsNullable    string
			DataType      string
			CharMaxLength *int
			NumPrecision  *int
			NumScale      *int
			DateTimePrec  *int
			COLUMN_KEY    *string
		}
		err = rows.Scan(
			&column.ColumnName,
			&column.ColumnDefault,
			&column.IsNullable,
			&column.DataType,
			&column.CharMaxLength,
			&column.NumPrecision,
			&column.NumScale,
			&column.DateTimePrec,
		)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		var precision, scale, nullable, isPK string
		createTableQuery := ""
		// Refer to:
		// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
		// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
		switch column.DataType {
		case "text", "longtext", "mediumtext", "tinytext", "blob", "longblob", "mediumblob", "tinyblob":
			createTableQuery += fmt.Sprintf("%s %s", column.ColumnName, "TEXT")
			precision = fmt.Sprintf("%d", *column.CharMaxLength)
		case "varchar", "char", "binary", "varbinary":
			createTableQuery += fmt.Sprintf("%s %s(%d)", column.ColumnName, strings.ToUpper(column.DataType), *column.CharMaxLength)
			precision = fmt.Sprintf("%d", *column.CharMaxLength)
		case "int", "mediumint":
			createTableQuery += fmt.Sprintf("%s %s", column.ColumnName, "INT")
			precision = fmt.Sprintf("%d", *column.NumPrecision)
		case "bigint", "tinyint", "smallint", "float", "double":
			createTableQuery += fmt.Sprintf("%s %s", column.ColumnName, strings.ToUpper(column.DataType))
			precision = fmt.Sprintf("%d", *column.NumPrecision)
		case "decimal", "numeric":
			createTableQuery += fmt.Sprintf("%s %s(%d, %d)", column.ColumnName, strings.ToUpper(column.DataType), *column.NumPrecision, *column.NumScale)
			precision = fmt.Sprintf("%d", *column.NumPrecision)
			scale = fmt.Sprintf("%d", *column.NumScale)
		case "bool", "boolean":
			createTableQuery += fmt.Sprintf("%s %s", column.ColumnName, "BOOLEAN")
			precision = "1"
		case "date":
			createTableQuery += fmt.Sprintf("%s %s", column.ColumnName, "DATE")
		case "datetime", "timestamp", "time":
			createTableQuery += fmt.Sprintf("%s %s(%d)", column.ColumnName, strings.ToUpper(column.DataType), *column.DateTimePrec)
			precision = fmt.Sprintf("%d", *column.DateTimePrec)
		default:
			fmt.Println("Unsupported data type: ", column.DataType)
		}
		if column.IsNullable == "NO" {
			createTableQuery += " NOT NULL"
			nullable = "false"
		} else {
			nullable = "true"
		}
		if column.ColumnDefault != nil {
			createTableQuery += fmt.Sprintf(` DEFAULT "%s"`, *column.ColumnDefault) // FIXME: Escape
		} else {
			createTableQuery += " DEFAULT NULL"
		}
		if column.COLUMN_KEY != nil && *column.COLUMN_KEY == "PRI" {
			isPK = "true"
		} else {
			isPK = "false"
		}
		tableCol = append(tableCol, cloudstorage.TableCol{
			Name:      column.ColumnName,
			Tp:        column.DataType,
			Default:   column.ColumnDefault,
			Precision: precision,
			Scale:     scale,
			Nullable:  nullable,
			IsPK:      isPK,
		})
		columnRows = append(columnRows, createTableQuery)
	}

	indexQuery := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", sourceDatabase, sourceTable) // FIXME: Escape
	indexRows, err := sourceTiDBConn.QueryContext(context.Background(), indexQuery)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	indexResults, err := export.GetSpecifiedColumnValuesAndClose(indexRows, "KEY_NAME", "COLUMN_NAME", "SEQ_IN_INDEX")
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	snowflakePKColumns := make([]string, 0)
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
			snowflakePKColumns = append(snowflakePKColumns, columnName)
		}
	}

	// TODO: Support unique key

	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	if len(snowflakePKColumns) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(snowflakePKColumns, ", ")))
	}
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE OR REPLACE TABLE %s (`, sourceTable)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), tableCol, nil
}

func GenMergeInto(tableDef cloudstorage.TableDefinition, filePath string, stageName string) string {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	selectStat = append(selectStat, `$1 AS "METADATA$FLAG"`)
	for i, col := range tableDef.Columns {
		selectStat = append(selectStat, fmt.Sprintf(`$%d AS %s`, i+5, col.Name))
	}

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

	// TODO: Remove QUALIFY row_number() after cdc support merge dml or snowflake support deterministic merge
	mergeQuery := fmt.Sprintf(
		`MERGE INTO %s AS T USING
		(
			SELECT
				%s
			FROM '@%s/%s'
			QUALIFY row_number() over (partition by %s order by $4 desc) = 1
		) AS S
		ON
		(
			%s
		)
		WHEN MATCHED AND S.METADATA$FLAG = 'U' THEN UPDATE SET %s
		WHEN MATCHED AND S.METADATA$FLAG = 'D' THEN DELETE
		WHEN NOT MATCHED AND S.METADATA$FLAG != 'D' THEN INSERT (%s) VALUES (%s);`,
		tableDef.Table,
		strings.Join(selectStat, ",\n"),
		stageName,
		filePath,
		strings.Join(pkColumn, ", "),
		strings.Join(onStat, " AND "),
		strings.Join(updateStat, ", "),
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "))

	return mergeQuery
}
