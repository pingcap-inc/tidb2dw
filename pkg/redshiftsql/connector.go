package redshiftsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
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
	// create stage
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
	if uri.Scheme == "file" {
		// if the file is local, we need to upload it to stage first
		putQuery := fmt.Sprintf(`PUT file://%s/%s '@%s/%s';`, uri.Path, filePath, rc.stageName, filePath)
		_, err := rc.db.Exec(putQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("put file to stage", zap.String("query", putQuery))
	}
	// TODO(lcui2): create external table, need S3 manifest file location
	manifestFilePath := ""
	externalTableName := fmt.Sprintf("%s", rc.stageName)
	externalTableSchema := fmt.Sprintf("%s_schema", rc.stageName)
	err := CreateExternalTable(rc.db, tableDef.Columns, externalTableName, externalTableSchema, manifestFilePath)

	// merge staged file into table
	err = GenDelete(rc.db, tableDef, rc.stageName)
	if err != nil {
		return errors.Trace(err)
	}

	err = GenInsert(rc.db, tableDef, rc.stageName)
	if err != nil {
		return errors.Trace(err)
	}

	deleteExternalTableQuery := fmt.Sprintf("DROP TABLE %s.%s", externalTableSchema, externalTableName)
	_, err = rc.db.Exec(deleteExternalTableQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("delete external table", zap.String("query", deleteExternalTableQuery))

	if uri.Scheme == "file" {
		// if the file is local, we need to remove it from stage
		removeQuery := fmt.Sprintf(`REMOVE '@%s/%s';`, rc.stageName, filePath)
		_, err = rc.db.Exec(removeQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("remove file from stage", zap.String("query", removeQuery))
	}

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}
