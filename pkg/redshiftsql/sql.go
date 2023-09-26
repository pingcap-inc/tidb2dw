package redshiftsql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"strings"

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

// LoadSnapshotFromS3 redshift currently can not support ROWS_PRODUCED function
// use csv file path for storageUri, like s3://tidbbucket/snapshot/stock.csv
func LoadSnapshotFromS3(db *sql.DB, targetTable, storageUri, filePrefix string, credential *credentials.Value, onSnapshotLoadProgress func(loadedRows int64)) error {
	sql, err := formatter.Format(`
	COPY {targetTable}
	FROM '{storageUrl}/{filePrefix}'
	CREDENTIALS 'aws_access_key_id={accessId};aws_secret_access_key={accessKey}'
	FORMAT AS CSV DELIMITER ',' QUOTE '"';
	`, formatter.Named{
		"targetTable": utils.EscapeString(targetTable),
		"storageUrl":  utils.EscapeString(storageUri),
		"filePrefix":  utils.EscapeString(filePrefix), // TODO: Verify
		"accessId":    credential.AccessKeyID,
		"accessKey":   credential.SecretAccessKey,
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Loading snapshot data from external table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DropTable(sourceTable string, db *sql.DB) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", sourceTable)
	log.Info("Dropping table in Redshift if exists", zap.String("query", sql))
	_, err := db.Exec(sql)
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

func CreateExternalSchema(db *sql.DB, schemaName, databaseName, iamRole string) error {
	sql, err := formatter.Format(`
	CREATE EXTERNAL SCHEMA IF NOT EXISTS {schemaName}
	FROM DATA CATALOG
	DATABASE '{databaseName}'
	IAM_ROLE '{iamRole}'
	CREATE EXTERNAL DATABASE IF NOT EXISTS;
	`, formatter.Named{
		"schemaName":   utils.EscapeString(schemaName),
		"databaseName": utils.EscapeString(databaseName),
		"iamRole":      utils.EscapeString(iamRole),
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
		"tableName":    utils.EscapeString(tableName),
		"schemaName":   utils.EscapeString(schemaName),
		"columns":      strings.Join(sqlRows, ",\n"),
		"manifestFile": utils.EscapeString(manifestFile),
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating external table", zap.String("query", sql))
	_, err = db.Exec(sql)
	return err
}

func DeleteQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, externalTableName string) error {
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
		"externalSchema": fmt.Sprintf("%s_schema", externalTableName),
		"externalTable":  externalTableName,
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

func InsertQuery(db *sql.DB, tableDef cloudstorage.TableDefinition, externalTableName string) error {
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
		flag, 
		{selectStat}
		FROM {externalSchema}.{externalTable} WHERE tablename IS NOT NULL
		QUALIFY row_number() OVER (PARTITION BY {pkStat} ORDER BY timestamp DESC) = 1
	) AS S
	WHERE
		S.flag != 'D'
	`, formatter.Named{
		"tableName":      tableDef.Table,
		"externalSchema": fmt.Sprintf("%s_schema", externalTableName),
		"externalTable":  externalTableName,
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
