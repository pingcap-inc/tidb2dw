package snowsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

// A Wrapper of snowflake connection.
// It implements the coreinterfaces.Connector interface.
type SnowflakeConnector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName string

	columns []cloudstorage.TableCol
}

func NewSnowflakeConnector(db *sql.DB, stageName string, storageURI *url.URL, credentials *credentials.Value) (*SnowflakeConnector, error) {
	// create stage
	var err error
	if storageURI.Host == "" {
		err = CreateInternalStage(db, stageName)
	} else {
		stageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
		err = CreateExternalStage(db, stageName, stageUrl, credentials)
	}
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	return &SnowflakeConnector{
		db:        db,
		stageName: stageName,
		columns:   nil,
	}, nil
}

func (sc *SnowflakeConnector) InitSchema(columns []cloudstorage.TableCol) error {
	if len(sc.columns) != 0 {
		return nil
	}
	if len(columns) == 0 {
		return errors.New("Columns in schema is empty")
	}
	sc.columns = columns
	log.Info("table columns initialized", zap.Any("Columns", columns))
	return nil
}

func (sc *SnowflakeConnector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if len(sc.columns) == 0 {
		return errors.New("Columns not initialized. Maybe you execute a DDL before all DMLs, which is not supported now.")
	}
	ddls, err := GenDDLViaColumnsDiff(sc.columns, tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("No need to execute this DDL in Snowflake", zap.String("ddl", tableDef.Query))
		return nil
	}
	// One DDL may be rewritten to multiple DDLs
	for _, ddl := range ddls {
		_, err := sc.db.Exec(ddl)
		if err != nil {
			log.Error("Failed to executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
			return errors.Annotate(err, fmt.Sprint("failed to execute", ddl))
		}
	}
	// update columns
	sc.columns = tableDef.Columns
	log.Info("Successfully executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
	return nil
}

func (sc *SnowflakeConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	createTableQuery, err := GenCreateSchema(sourceDatabase, sourceTable, sourceTiDBConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Snowflake", zap.String("query", createTableQuery))
	_, err = sc.db.Exec(createTableQuery)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

func (sc *SnowflakeConnector) LoadSnapshot(targetTable, filePrefix string, onSnapshotLoadProgress func(loadedRows int64)) error {
	if err := LoadSnapshotFromStage(sc.db, targetTable, sc.stageName, filePrefix, onSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully load snapshot", zap.String("table", targetTable), zap.String("filePrefix", filePrefix))
	return nil
}

func (sc *SnowflakeConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	if uri.Scheme == "file" {
		// if the file is local, we need to upload it to stage first
		putQuery := fmt.Sprintf(`PUT file://%s/%s '@%s/%s';`, uri.Path, filePath, sc.stageName, filePath)
		_, err := sc.db.Exec(putQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("put file to stage", zap.String("query", putQuery))
	}

	// merge staged file into table
	mergeQuery := GenMergeInto(tableDef, filePath, sc.stageName)
	_, err := sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("merge staged file into table", zap.String("query", mergeQuery))

	if uri.Scheme == "file" {
		// if the file is local, we need to remove it from stage
		removeQuery := fmt.Sprintf(`REMOVE '@%s/%s';`, sc.stageName, filePath)
		_, err = sc.db.Exec(removeQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("remove file from stage", zap.String("query", removeQuery))
	}

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *SnowflakeConnector) Clone(stageName string, storageURI *url.URL, credentials *credentials.Value) (coreinterfaces.Connector, error) {
	return NewSnowflakeConnector(sc.db, stageName, storageURI, credentials)
}

func (sc *SnowflakeConnector) Close() {
	// drop stage
	if err := DropStage(sc.db, sc.stageName); err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	}
	sc.db.Close()
}
