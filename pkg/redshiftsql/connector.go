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
	db *sql.DB

	stageName  string
	storageUrl string
	credential *credentials.Value
	columns    []cloudstorage.TableCol
}

func NewRedshiftConnector(db *sql.DB, stageName string, storageURI *url.URL, credentials *credentials.Value) (*RedshiftConnector, error) {
	// create stage
	var err error
	storageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	var mode = strings.Split(stageName, "_")[0]
	if mode == "increment" {
		// TODO(lcui2): create external schema, need iam role
		// TODO(lcui2): create external tabnle, need S3 file location
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
