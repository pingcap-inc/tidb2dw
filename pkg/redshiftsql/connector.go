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
	storageUrl    *url.URL
	s3Credentials *credentials.Value
	iamRole       string
	columns       []cloudstorage.TableCol
}

func NewRedshiftConnector(db *sql.DB, schemaName, externalTableName, iamRole string, storageURI *url.URL, s3Credentials *credentials.Value) (*RedshiftConnector, error) {
	var err error
	// create schema
	err = CreateSchema(db, schemaName)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create schema")
	}
	// need iam role to create external schema
	var mode = strings.Split(externalTableName, "_")[0]
	if mode == "increment" {
		err = CreateExternalSchema(db, fmt.Sprintf("%s_schema", externalTableName), fmt.Sprintf("%s_database", externalTableName), iamRole)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to create external table")
		}
	} else if mode != "snapshot" {
		return nil, errors.Annotate(err, "Incorrect external table name, only support snapshot_* and increment_*")
	}
	return &RedshiftConnector{
		db:            db,
		schemaName:    schemaName,
		tableName:     externalTableName,
		storageUrl:    storageURI,
		s3Credentials: s3Credentials,
		iamRole:       iamRole,
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
	err := DropTable(sourceTable, rc.db)
	if err != nil {
		return errors.Trace(err)
	}
	err = CreateTable(sourceDatabase, sourceTable, sourceTiDBConn, rc.db)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

// filePrefix should be
func (rc *RedshiftConnector) LoadSnapshot(targetTable, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error {
	filePrefix = fmt.Sprintf("%s://%s/%s", rc.storageUrl.Scheme, rc.storageUrl.Host, filePrefix)
	if err := LoadSnapshotFromS3(rc.db, targetTable, filePrefix, rc.s3Credentials, onSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePrefix", filePrefix))
	return nil
}

func (rc *RedshiftConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error {
	// create external table, need S3 manifest file location
	externalTableName := rc.tableName
	externalTableSchema := fmt.Sprintf("%s_schema", rc.tableName)
	fileSuffix := filepath.Ext(filePath)
	manifestFilePath := fmt.Sprintf("%s://%s%s/%s.manifest", rc.storageUrl.Scheme, rc.storageUrl.Host, rc.storageUrl.Path, strings.TrimSuffix(filePath, fileSuffix))
	err := CreateExternalTable(rc.db, tableDef.Columns, externalTableName, externalTableSchema, manifestFilePath)
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

	err = DeleteTable(rc.db, externalTableSchema, externalTableName)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (rc *RedshiftConnector) Close() {
	// drop schema
	schemaName := fmt.Sprintf("%s_schema", rc.tableName)
	if err := DropExternalSchema(rc.db, schemaName); err != nil {
		log.Error("fail to drop schema", zap.Error(err))
	}
	rc.db.Close()
}
