package redshiftsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

type RedshiftConnector struct {
	// db is the connection to redshift.
	db         *sql.DB
	stageName  string
	storageUrl string
	credential *credentials.Value
	iamRole    string
	columns    []cloudstorage.TableCol
}

func NewRedshiftConnector(db *sql.DB, stageName string, storageURI *url.URL, credentials *credentials.Value) (*RedshiftConnector, error) {
	var err error
	storageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	// need iam role to create external schema
	iamRole := os.Getenv("IAM_ROLE")
	var mode = strings.Split(stageName, "_")[0]
	if mode == "increment" {
		// create external schema, need iam role
		err = CreateExternalSchema(db, fmt.Sprintf("%s_schema", stageName), fmt.Sprintf("%s_database", stageName), iamRole)

	} else if mode != "snapshot" {
		return nil, errors.Annotate(err, "Incorrect stage name, only support snapshot_stage_* and increment_stage_")
	}
	// do not need to create stage for snapshot replication
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	return &RedshiftConnector{
		db:         db,
		stageName:  stageName,
		storageUrl: storageUrl,
		credential: credentials,
		iamRole:    iamRole,
		columns:    nil,
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
	dropTableQuery, err := GenDropSchema(sourceTable)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Dropping table in Redshift if exists", zap.String("query", dropTableQuery))
	_, err = rc.db.Exec(dropTableQuery)
	if err != nil {
		return errors.Trace(err)
	}
	createTableQuery, err := GenCreateSchema(sourceDatabase, sourceTable, sourceTiDBConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Redshift", zap.String("query", createTableQuery))
	_, err = rc.db.Exec(createTableQuery)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

// filePrefix should be
func (rc *RedshiftConnector) LoadSnapshot(targetTable, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error {
	if err := LoadSnapshotFromStage(rc.db, targetTable, rc.storageUrl, filePrefix, rc.credential, onSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePrefix", filePrefix))
	return nil
}

func (rc *RedshiftConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	// redshift does not support PUT command
	// TODO(lcui2): what if uri.Scheme == "file"

	// create external table, need S3 manifest file location
	// TODO(lcui2): need to check the path for manifest file
	manifestFilePath := ""
	externalTableName := fmt.Sprintf("%s", rc.stageName)
	externalTableSchema := fmt.Sprintf("%s_schema", rc.stageName)
	err := CreateExternalTable(rc.db, tableDef.Columns, externalTableName, externalTableSchema, manifestFilePath)

	// merge staged file into table
	err = DeleteQuery(rc.db, tableDef, rc.stageName)
	if err != nil {
		return errors.Trace(err)
	}

	err = InsertQuery(rc.db, tableDef, rc.stageName)
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

func (rc *RedshiftConnector) Clone(stageName string, storageURI *url.URL, credentials *credentials.Value) (coreinterfaces.Connector, error) {
	return NewRedshiftConnector(rc.db, stageName, storageURI, credentials)
}

func (rc *RedshiftConnector) Close() {
	// drop schema
	schemaName := fmt.Sprintf("%s_schema", rc.stageName)
	if err := DropExternalSchema(rc.db, schemaName); err != nil {
		log.Error("fail to drop schema", zap.Error(err))
	}
	rc.db.Close()
}
