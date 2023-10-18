package redshiftsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

type RedshiftConnector struct {
	// db is the connection to redshift.
	db            *sql.DB
	schemaName    string
	tableName     string
	storageUri    *url.URL
	s3Credentials *credentials.Value
	columns       []cloudstorage.TableCol
}

func NewRedshiftConnector(db *sql.DB, schemaName, externalTableName string, storageURI *url.URL, s3Credentials *credentials.Value) (*RedshiftConnector, error) {
	var err error
	// create schema
	err = CreateSchema(db, schemaName)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create schema")
	}
	if err = CreateExternalSchema(db, fmt.Sprintf("%s_schema", externalTableName), fmt.Sprintf("%s_database", externalTableName)); err != nil {
		return nil, errors.Annotate(err, "Failed to create external table")
	}
	return &RedshiftConnector{
		db:            db,
		schemaName:    schemaName,
		tableName:     externalTableName,
		storageUri:    storageURI,
		s3Credentials: s3Credentials,
		columns:       nil,
	}, nil
}

func (rc *RedshiftConnector) InitSchema(columns []cloudstorage.TableCol) error {
	if len(rc.columns) != 0 {
		return nil
	}
	if len(columns) == 0 {
		return errors.New("Columns in schema is empty")
	}
	rc.columns = columns
	log.Info("table columns initialized", zap.Any("Columns", columns))
	return nil
}

func (rc *RedshiftConnector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if len(rc.columns) == 0 {
		return errors.New("Columns not initialized. Maybe you execute a DDL before all DMLs, which is not supported now.")
	}
	ddls, err := GenDDLViaColumnsDiff(rc.columns, tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("No need to execute this DDL in Redshift", zap.String("ddl", tableDef.Query))
		return nil
	}
	// One DDL may be rewritten to multiple DDLs
	for _, ddl := range ddls {
		_, err := rc.db.Exec(ddl)
		if err != nil {
			log.Error("Failed to executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
			return errors.Annotate(err, fmt.Sprint("failed to execute", ddl))
		}
	}
	// update columns
	rc.columns = tableDef.Columns
	log.Info("Successfully executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
	return nil
}

func (rc *RedshiftConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	err := DropTable(rc.db, sourceTable, "")
	if err != nil {
		return errors.Trace(err)
	}
	return CreateTable(sourceDatabase, sourceTable, sourceTiDBConn, rc.db)
}

func (rc *RedshiftConnector) LoadSnapshot(targetTable, filePath string) error {
	filePath = fmt.Sprintf("%s://%s%s/%s", rc.storageUri.Scheme, rc.storageUri.Host, rc.storageUri.Path, filePath)
	if err := LoadSnapshotFromS3(rc.db, targetTable, filePath, rc.s3Credentials); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rc *RedshiftConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	// create external table, need S3 manifest file location
	externalTableName := rc.tableName
	externalTableSchema := fmt.Sprintf("%s_schema", rc.tableName)
	fileSuffix := filepath.Ext(filePath)
	manifestFilePath := fmt.Sprintf("%s://%s%s/%s.manifest", uri.Scheme, uri.Host, uri.Path, strings.TrimSuffix(filePath, fileSuffix))
	// drop external table if exists
	err := DropTable(rc.db, externalTableName, externalTableSchema)
	if err != nil {
		return errors.Trace(err)
	}
	err = CreateExternalTable(rc.db, tableDef.Columns, externalTableName, externalTableSchema, manifestFilePath)
	if err != nil {
		return errors.Trace(err)
	}

	// merge external table file into table
	err = DeleteQuery(rc.db, tableDef, rc.tableName)
	if err != nil {
		return errors.Trace(err)
	}

	err = InsertQuery(rc.db, tableDef, rc.tableName)
	if err != nil {
		return errors.Trace(err)
	}

	err = DropTable(rc.db, externalTableName, externalTableSchema)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (rc *RedshiftConnector) Close() {
	if err := DropExternalSchema(rc.db, rc.tableName); err != nil {
		log.Error("fail to drop schema", zap.Error(err))
	}
	rc.db.Close()
}
