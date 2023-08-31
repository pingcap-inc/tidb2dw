package replicate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type SnapshotReplicateSession struct {
	TiDBConfig *tidbsql.TiDBConfig

	DataWarehousePool coreinterfaces.Connector
	TiDBPool          *sql.DB

	SourceDatabase string
	SourceTable    string
	StartTSO       string

	OnSnapshotLoadProgress func(loadedRows int64)

	StorageWorkspaceUri url.URL
}

func NewSnapshotReplicateSession(
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	sourceDatabase, sourceTable string,
	storageUri *url.URL,
	startTSO string,
) (*SnapshotReplicateSession, error) {
	sess := &SnapshotReplicateSession{
		DataWarehousePool:   dwConnector,
		TiDBConfig:          tidbConfig,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		StartTSO:            startTSO,
		StorageWorkspaceUri: *storageUri,
	}
	log.Info("Creating replicate session",
		zap.String("storage", sess.StorageWorkspaceUri.String()),
		zap.String("source", fmt.Sprintf("%s.%s", sourceDatabase, sourceTable)))
	{
		db, err := tidbConfig.OpenDB()
		if err != nil {
			return nil, errors.Trace(err)
		}
		sess.TiDBPool = db
	}
	// Setup progress reporters
	sess.OnSnapshotLoadProgress = func(loadedRows int64) {
		log.Info("Snapshot load progress", zap.Int64("loadedRows", loadedRows))
	}

	return sess, nil
}

func (sess *SnapshotReplicateSession) Close() {
	if sess.DataWarehousePool != nil {
		sess.DataWarehousePool.Close()
	}
	if sess.TiDBPool != nil {
		sess.TiDBPool.Close()
	}
}

func (sess *SnapshotReplicateSession) Run() error {
	switch sess.StorageWorkspaceUri.Scheme {
	case "s3":
		if err := sess.DataWarehousePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
			return errors.Trace(err)
		}
	case "gcs":
		log.Error("GCS does not supprt data warehouse connector now...")
		return errors.New("GCS does not supprt data warehouse connector now...")
	}

	startTime := time.Now()
	if err := sess.loadSnapshotDataIntoDataWarehouse(); err != nil {
		return errors.Annotate(err, "Failed to load snapshot data into data warehouse")
	}
	endTime := time.Now()

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into data warehouse.
	ctx := context.Background()
	storage, err := putil.GetExternalStorageFromURI(ctx, sess.StorageWorkspaceUri.String())
	if err != nil {
		log.Error("Failed to get external storage", zap.Error(err))
	}
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err = storage.WriteFile(ctx, "loadinfo", []byte(loadinfo)); err != nil {
		log.Error("Failed to upload loadinfo", zap.Error(err))
	}
	log.Info("Successfully upload loadinfo", zap.String("loadinfo", loadinfo))
	return nil
}

func (sess *SnapshotReplicateSession) loadSnapshotDataIntoDataWarehouse() error {
	workspacePrefix := strings.TrimPrefix(sess.StorageWorkspaceUri.Path, "/")
	dumpFilePrefix := fmt.Sprintf("%s/%s.%s.", workspacePrefix, sess.SourceDatabase, sess.SourceTable)
	if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, dumpFilePrefix, sess.OnSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func StartReplicateSnapshot(
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	tableFQN string,
	storageUri *url.URL,
	startTSO string,
) error {
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	session, err := NewSnapshotReplicateSession(dwConnector, tidbConfig, sourceDatabase, sourceTable, storageUri, startTSO)
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()
	if err := session.Run(); err != nil {
		return errors.Trace(err)
	}
	return nil
}
