package snowsql

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	_ "github.com/snowflakedb/gosnowflake"
	"go.uber.org/zap"
)

type SnowflakeConnector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName string
}

func NewSnowflakeConnector(uri string, stageName string, upstreamURI *url.URL, credentials credentials.Value) (*SnowflakeConnector, error) {
	db, err := sql.Open("snowflake", uri)
	if err != nil {
		log.Error("fail to connect to snowflake", zap.Error(err))
	}
	// make sure the connection is available
	err = db.Ping()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("snowflake connection established")

	// create stage
	if upstreamURI.Host == "" {
		err = CreateInternalStage(db, stageName)
	} else {
		stageUrl := fmt.Sprintf("%s://%s%s", upstreamURI.Scheme, upstreamURI.Host, upstreamURI.Path)
		err = CreateExternalStage(db, stageName, stageUrl, credentials)
	}
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	return &SnowflakeConnector{db, stageName}, nil
}

func (sc *SnowflakeConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	createTableQuery, err := GenCreateSchema(sourceDatabase, sourceTable, sourceTiDBConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Snowflake", zap.String("sql", createTableQuery))
	_, err = sc.db.Exec(createTableQuery)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Successfully copying table scheme", zap.String("database", sourceDatabase), zap.String("table", sourceTable))
	return nil
}

func (sc *SnowflakeConnector) CopyFile(targetTable, fileName string) error {
	copyQuery := GenLoadSnapshotFromStage(targetTable, sc.stageName, fileName)
	_, err := sc.db.Exec(copyQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully copy file", zap.String("file", fileName))
	return nil
}

func (sc *SnowflakeConnector) MergeFile(tableDef cloudstorage.TableDefinition, uri *url.URL, filePath string) error {
	if uri.Scheme == "file" {
		// if the file is local, we need to upload it to stage first
		putQuery := fmt.Sprintf(`PUT file://%s/%s '@"%s"/%s';`, uri.Path, filePath, sc.stageName, filePath)
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
		removeQuery := fmt.Sprintf(`REMOVE '@"%s"/%s';`, sc.stageName, filePath)
		_, err = sc.db.Exec(removeQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("remove file from stage", zap.String("query", removeQuery))
	}

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *SnowflakeConnector) Close() {
	// drop stage
	err := DropStage(sc.db, sc.stageName)
	if err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	}

	sc.db.Close()
}
