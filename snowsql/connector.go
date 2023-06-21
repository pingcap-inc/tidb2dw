package snowsql

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	_ "github.com/snowflakedb/gosnowflake"
	"go.uber.org/zap"
)

type SnowflakeConnector struct {
	// db is the connection to snowflake.
	db *sql.DB

	tableDef cloudstorage.TableDefinition

	stageName          string
	storageIntegration string
}

func NewSnowflakeConnector(uri string, tableDef cloudstorage.TableDefinition, upstreamURI *url.URL, storageIntegration string) (*SnowflakeConnector, error) {
	db, err := sql.Open("snowflake", uri)
	if err != nil {
		log.Error("fail to connect to snowflake", zap.Error(err))
	}
	// make sure the connection is available
	err = db.Ping()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("snowflake connection established", zap.String("uri", uri))

	// create stage
	stageName := fmt.Sprintf("cdc_stage_%s", tableDef.Table)
	createStageQuery := GenCreateStage(stageName, upstreamURI.Path, storageIntegration)
	_, err = db.Exec(createStageQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SnowflakeConnector{db, tableDef, stageName, storageIntegration}, nil
}

func (sc *SnowflakeConnector) MergeFile(uri *url.URL, filePath string) error {
	if uri.Scheme == "file" {
		// if the file is local, we need to upload it to stage first
		putQuery := fmt.Sprintf(`PUT %s://%s/%s '@%s/%s';`, uri.Scheme, uri.Path, filePath, sc.tableDef.Table, filePath)
		_, err := sc.db.Exec(putQuery)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("put file to stage", zap.String("query", putQuery))
	}

	// merge staged file into table
	mergeQuery := GenMergeInto(sc.tableDef, filePath)
	_, err := sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("merge staged file into table", zap.String("query", mergeQuery))

	if uri.Scheme == "file" {
		// if the file is local, we need to remove it from stage
		removeQuery := fmt.Sprintf(`REMOVE '@%s/%s';`, sc.tableDef.Table, filePath)
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
	dropStageQuery := GenDropStage(sc.stageName)
	_, err := sc.db.Exec(dropStageQuery)
	if err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	}

	sc.db.Close()
}
