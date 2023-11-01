package redshiftsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

type RedshiftConnector struct {
	// db is the connection to redshift.
	db             *sql.DB
	increTableName string
	storageUri     *url.URL
	s3Credentials  *credentials.Value
	columns        []cloudstorage.TableCol
}

func NewRedshiftConnector(rsConfig *RedshiftConfig, increTableName string, storageURI *url.URL, s3Credentials *credentials.Value) (*RedshiftConnector, error) {
	db, err := rsConfig.OpenDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RedshiftConnector{
		db:             db,
		increTableName: increTableName,
		storageUri:     storageURI,
		s3Credentials:  s3Credentials,
		columns:        nil,
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
	if err := DropTableIfExists(rc.db, sourceTable); err != nil {
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

func (rc *RedshiftConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error {
	// create incremental table
	if err := CreateIncrementalTable(rc.db, tableDef.Columns, rc.increTableName); err != nil {
		return errors.Trace(err)
	}
	// There are may be DDLs executed in upstream, so the schema may be changed.
	// Drop the table here instead of truncating the table.
	defer DropTableIfExists(rc.db, rc.increTableName)

	// merge incremental table file into table
	// 1. copy data from S3 to temp table
	// 2. delete rows with same pk in temp table from source table
	// 3. insert rows in temp table to source table
	filePath = fmt.Sprintf("%s://%s%s/%s", rc.storageUri.Scheme, rc.storageUri.Host, rc.storageUri.Path, filePath)
	if err := LoadSnapshotFromS3(rc.db, rc.increTableName, filePath, rc.s3Credentials); err != nil {
		return errors.Trace(err)
	}
	if err := DeleteQuery(rc.db, tableDef, rc.increTableName); err != nil {
		return errors.Trace(err)
	}
	if err := InsertQuery(rc.db, tableDef, rc.increTableName); err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (rc *RedshiftConnector) Close() {
	rc.db.Close()
}
