package redshiftsql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/utils"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"gitlab.com/tymonx/go-formatter/formatter"
	"go.uber.org/zap"
)

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

// LoadSnapshotFromS3 load snapshot data from s3 to redshift
// use csv file path for storageUri, like s3://tidbbucket/snapshot/stock.csv
func LoadSnapshotFromS3(db *sql.DB, targetTable, filePath string, credential *credentials.Value) error {
	sql, err := formatter.Format(`
	COPY {targetTable}
	FROM '{filePath}'
	CREDENTIALS 'aws_access_key_id={accessId};aws_secret_access_key={accessKey}'
	FORMAT AS CSV DELIMITER ',' QUOTE '"' NULL AS '\\N';
	`, formatter.Named{
		"targetTable": utils.EscapeString(targetTable),
		"filePath":    utils.EscapeString(filePath), // TODO: Verify
		"accessId":    credential.AccessKeyID,
		"accessKey":   credential.SecretAccessKey,
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Loading snapshot data from external storage", zap.String("filepath", filePath), zap.String("table", targetTable))
	_, err = db.Exec(sql)
	return err
}

func CreateTable(sourceDatabase string, sourceTable string, sourceTiDBConn, redConn *sql.DB) error {
	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return errors.Trace(err)
	}
	columnRows := make([]string, 0, len(tableColumns))
	for _, column := range tableColumns {
		row, err := GetRedshiftColumnString(column)
		if err != nil {
			return errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	redshiftPKColumns, err := tidbsql.GetTiDBTablePKColumns(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return errors.Trace(err)
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

	query := strings.Join(sql, "\n")
	log.Info("Creating table in Redshift", zap.String("query", query))
	_, err = redConn.Exec(query)
	return err
}

// CreateIncrementalTable create a temp table to store incremental data
// Note: We do not use external table here, because redshift external table has a lot of limitations:
// 1. Does not support NOT NULL or PRIMARY KEY
// 2. Does not support specifying null value
// 3. Need a manifest file to specify the file location
// 4. ...
func CreateIncrementalTable(db *sql.DB, columns []cloudstorage.TableCol, tableName string) error {
	columnRows := make([]string, 0, len(columns))
	for _, column := range columns {
		row, err := GetRedshiftColumnString(column)
		if err != nil {
			return errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	sql, err := formatter.Format(`
	CREATE TEMP TABLE IF NOT EXISTS {tableName} (
		FLAG VARCHAR(10),
		TABLENAME VARCHAR(255),
		SCHEMANAME VARCHAR(255),
		COMMITTS BIGINT,
		{columns}
	)
	`, formatter.Named{
		"tableName": utils.EscapeString(tableName),
		"columns":   strings.Join(columnRows, ",\n"),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating incurmental table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DeleteQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, incurmentalTableName string) error {
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
		{pkStat}
		FROM {incurmentalTableName} WHERE tablename IS NOT NULL
		QUALIFY row_number() OVER (PARTITION BY {pkStat} ORDER BY committs DESC) = 1
	) AS S
	WHERE 
		{onStat};
	`, formatter.Named{
		"tableName":            tableDef.Table,
		"incurmentalTableName": incurmentalTableName,
		"pkStat":               strings.Join(pkColumn, ", "),
		"onStat":               strings.Join(onStat, " AND "),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("delete incurmental table from source table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func InsertQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, incurmentalTableName string) error {
	selectStat := make([]string, 0, len(tableDef.Columns))
	pkColumn := make([]string, 0)
	for _, col := range tableDef.Columns {
		selectStat = append(selectStat, col.Name)
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
		flag, 
		{selectStat}
		FROM {incurmentalTableName} WHERE tablename IS NOT NULL
		QUALIFY row_number() OVER (PARTITION BY {pkStat} ORDER BY committs DESC) = 1
	) AS S
	WHERE
		S.flag != 'D'
	`, formatter.Named{
		"tableName":            tableDef.Table,
		"incurmentalTableName": incurmentalTableName,
		"selectStat":           strings.Join(selectStat, ", "),
		"pkStat":               strings.Join(pkColumn, ", "),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("insert incurmental table into source table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DropTableIfExists(db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	log.Info("drop table", zap.String("query", sql))
	_, err := db.Exec(sql)
	return err
}
