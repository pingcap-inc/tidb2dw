package snowsql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/snowflakedb/gosnowflake"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
)

func CreateExternalStage(db *sql.DB, stageName, s3WorkspaceURL string, cred *credentials.Value) error {
	sql, err := formatter.Format(`
CREATE OR REPLACE STAGE {stageName}
URL = '{url}'
CREDENTIALS = (AWS_KEY_ID = '{awsKeyId}' AWS_SECRET_KEY = '{awsSecretKey}' AWS_TOKEN = '{awsToken}')
FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"');
	`, formatter.Named{
		"stageName":    utils.EscapeString(stageName),
		"url":          utils.EscapeString(s3WorkspaceURL),
		"awsKeyId":     utils.EscapeString(cred.AccessKeyID),
		"awsSecretKey": utils.EscapeString(cred.SecretAccessKey),
		"awsToken":     utils.EscapeString(cred.SessionToken),
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
		"stageName": utils.EscapeString(stageName),
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
		"stageName": utils.EscapeString(stageName),
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
PATTERN = '.*{filePrefix}.*\.csv'
ON_ERROR = CONTINUE;
`, formatter.Named{
		"reqId":       utils.EscapeString(reqId.String()),
		"targetTable": utils.EscapeString(targetTable),
		"stageName":   utils.EscapeString(stageName),
		"filePrefix":  utils.EscapeString(regexp.QuoteMeta(filePrefix)),
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

func GetDefaultValueString(val string) string {
	_, err := strconv.ParseFloat(fmt.Sprint(val), 64)
	if err != nil {
		return fmt.Sprintf("'%s'", val) // FIXME: escape
	}
	return fmt.Sprint(val)
}

func GenCreateSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) (string, error) {
	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	columnRows := make([]string, 0, len(tableColumns))
	for _, column := range tableColumns {
		row, err := GetSnowflakeColumnString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	snowflakePKColumns, err := tidbsql.GetTiDBTablePKColumns(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return "", errors.Trace(err)
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

	return strings.Join(sql, "\n"), nil
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
		WHEN MATCHED AND S.METADATA$FLAG != 'D' THEN UPDATE SET %s
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
