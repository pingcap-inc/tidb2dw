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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
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

func CreateSchema(db *sql.DB, schemaName string) error {
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName)
	_, err := db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	sql = fmt.Sprintf("SET search_path TO %s", schemaName)
	_, err = db.Exec(sql)
	return err
}

// redshift currently can not support ROWS_PRODUCED function
// use csv file path for stageUrl, like s3://tidbbucket/snapshot/stock.csv
func LoadSnapshotFromStage(db *sql.DB, targetTable, storageUrl, filePrefix string, credential *credentials.Value, onSnapshotLoadProgress func(loadedRows int64)) error {
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
		row, err := GetRedshiftColumnString(column)
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

func CreateExternalSchema(db *sql.DB, schemaName, databaseName, iamRole string) error {
	sql, err := formatter.Format(`
	CREATE EXTERNAL SCHEMA IF NOT EXISTS {schemaName}
	FROM DATA CATALOG
	DATABASE '{databaseName}'
	IAM_ROLE '{iamRole}'
	CREATE EXTERNAL DATABASE IF NOT EXISTS;
	`, formatter.Named{
		"schemaName":   snowsql.EscapeString(schemaName),
		"databaseName": snowsql.EscapeString(databaseName),
		"iamRole":      snowsql.EscapeString(iamRole),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating external schema", zap.String("query", sql))
	ctx := context.Background()
	_, err = db.ExecContext(ctx, sql)

	return err
}

// Redshift external table does not support NOT NULL or PRIMARY KEY
func CreateExternalTable(db *sql.DB, columns []cloudstorage.TableCol, tableName, schemaName, manifestFile string) error {
	columnRows := make([]string, 0, len(columns))
	for _, column := range columns {
		row, err := GetRedshiftTypeString(column)
		if err != nil {
			return errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}
	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	for i := range columnRows {
		log.Info("curr column info:", zap.String("col:", columnRows[i]))
	}

	// sql := []string{}
	sql, err := formatter.Format(`
	CREATE EXTERNAL TABLE {schemaName}.{tableName} (
		FLAG VARCHAR(10),
		TABLENAME VARCHAR(255),
		SCHEMANAME VARCHAR(255),
		TIMESTAMP VARCHAR(255),
		{columns}
	)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED by ','
	LINES TERMINATED BY '\n'
	LOCATION '{manifestFile}'
	`, formatter.Named{
		"tableName":    snowsql.EscapeString(tableName),
		"schemaName":   snowsql.EscapeString(schemaName),
		"columns":      strings.Join(sqlRows, ",\n"),
		"manifestFile": snowsql.EscapeString(manifestFile),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating external table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DeleteQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, stageName string) error {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	selectStat = append(selectStat, `flag`)
	for _, col := range tableDef.Columns {
		selectStat = append(selectStat, col.Name)
	}
	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
			onStat = append(onStat, fmt.Sprintf(`%s.%s = S.%s`, tableDef.Table, col.Name, col.Name))
		}
	}
	sql, err := formatter.Format(`
	DELETE FROM {tableName} USING (
		SELECT
		{selectStat}
		FROM {externalSchema}.{externalTable} WHERE tablename IS NOT NULL
		QUALIFY row_number() OVER (PARTITION BY {pkStat} ORDER BY timestamp DESC) = 1
	) AS S
	WHERE 
		{onStat};
	`, formatter.Named{
		"tableName":      tableDef.Table,
		"externalSchema": fmt.Sprintf("%s_schema", stageName),
		"externalTable":  fmt.Sprintf("%s", stageName),
		"selectStat":     strings.Join(selectStat, ",\n"),
		"pkStat":         strings.Join(pkColumn, ", "),
		"onStat":         strings.Join(onStat, " AND "),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("delete external table into table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func InsertQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, stageName string) error {
	selectStat := make([]string, 0, len(tableDef.Columns)+1)
	for _, col := range tableDef.Columns {
		selectStat = append(selectStat, col.Name)
	}
	pkColumn := make([]string, 0)

	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
		}
	}
	sql, err := formatter.Format(`
	INSERT INTO {tableName}  
	SELECT
		{selectStat}
	FROM (
	SELECT
		flag, {selectStat}
		FROM {externalSchema}.{externalTable} WHERE tablename IS NOT NULL
		QUALIFY row_number() OVER (PARTITION BY {pkStat} ORDER BY timestamp DESC) = 1
	) AS S
	WHERE
		S.flag != 'D'
	`, formatter.Named{
		"tableName":      tableDef.Table,
		"externalSchema": fmt.Sprintf("%s_schema", stageName),
		"externalTable":  fmt.Sprintf("%s", stageName),
		"selectStat":     strings.Join(selectStat, ",\n"),
		"pkStat":         strings.Join(pkColumn, ", "),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("insert external table into table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DeleteTable(db *sql.DB, tableName, schemaName string) error {
	sql := fmt.Sprintf("DROP TABLE %s.%s", tableName, schemaName)
	log.Info("delete table", zap.String("query", sql))
	_, err := db.Exec(sql)
	return err
}

func DropExternalSchema(db *sql.DB, schemaName string) error {
	sql := fmt.Sprintf("DROP SCHEMA IF EXISTS %s DROP EXTERNAL DATABASE CASCADE", schemaName)
	_, err := db.Exec(sql)
	return err
}
