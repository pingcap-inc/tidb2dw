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
}

func NewSnowflakeConnector(uri string, tableDef cloudstorage.TableDefinition, fileFormat string) (*SnowflakeConnector, error) {
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
	createStageQuery := fmt.Sprintf(`CREATE OR REPLACE STAGE "%s" FILE_FORMAT = %s;`, tableDef.Table, fileFormat)
	_, err = db.Exec(createStageQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SnowflakeConnector{db, tableDef}, nil
}

func (sc *SnowflakeConnector) MergeFile(uri *url.URL, filePath string, fileFormat string) error {
	// put file to stage
	putQuery := fmt.Sprintf(`PUT %s://%s/%s @"%s";`, uri.Scheme, uri.Path, filePath, sc.tableDef.Table)
	_, err := sc.db.Exec(putQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("put file to stage", zap.String("query", putQuery))

	// merge staged file into table
	mergeQuery := GenMergeInto(sc.tableDef, fileFormat)
	_, err = sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("merge staged file into table", zap.String("query", mergeQuery))

	// remove file from stage
	removeQuery := fmt.Sprintf(`REMOVE '@"%s"' pattern ='.*CDC[0-9]*.csv.*';`, sc.tableDef.Table)
	_, err = sc.db.Exec(removeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("remove file from stage", zap.String("query", removeQuery))

	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *SnowflakeConnector) Close() {
	sc.db.Close()
}
