package replicate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type SnapshotReplicateSession struct {
	TiDBConfig *tidbsql.TiDBConfig

	DataWarehousePool coreinterfaces.Connector
	TiDBPool          *sql.DB

	SourceDatabase string
	SourceTable    string

	OnSnapshotLoadProgress func(loadedRows int64)

	StorageWorkspaceUri url.URL
	externalStorage     storage.ExternalStorage

	ctx    context.Context
	logger *zap.Logger
}

func NewSnapshotReplicateSession(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	sourceDatabase, sourceTable string,
	storageUri *url.URL,
	logger *zap.Logger,
) (*SnapshotReplicateSession, error) {
	sess := &SnapshotReplicateSession{
		DataWarehousePool:   dwConnector,
		TiDBConfig:          tidbConfig,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		StorageWorkspaceUri: *storageUri,
		ctx:                 ctx,
		logger:              logger,
	}
	sess.logger.Info("Creating replicate session",
		zap.String("storage", sess.StorageWorkspaceUri.String()),
		zap.String("source", fmt.Sprintf("%s.%s", sourceDatabase, sourceTable)))
	{
		db, err := tidbConfig.OpenDB()
		if err != nil {
			return nil, errors.Trace(err)
		}
		sess.TiDBPool = db
	}
	{
		// Setup progress reporters
		sess.OnSnapshotLoadProgress = func(loadedRows int64) {
			sess.logger.Info("Snapshot load progress", zap.Int64("loadedRows", loadedRows))
		}
	}
	{
		externalStorage, err := putil.GetExternalStorageFromURI(sess.ctx, storageUri.String())
		if err != nil {
			return nil, errors.Trace(err)
		}
		sess.externalStorage = externalStorage
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
	case "s3", "gcs", "gs":
		if err := sess.DataWarehousePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.Errorf("%s does not supprt data warehouse connector now...", sess.StorageWorkspaceUri.Scheme)
	}

	startTime := time.Now()
	if err := sess.loadSnapshotDataIntoDataWarehouse(); err != nil {
		return errors.Annotate(err, "Failed to load snapshot data into data warehouse")
	}
	endTime := time.Now()

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into data warehouse.
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err := sess.externalStorage.WriteFile(sess.ctx, "loadinfo", []byte(loadinfo)); err != nil {
		sess.logger.Error("Failed to upload loadinfo", zap.Error(err))
	}
	sess.logger.Info("Successfully upload loadinfo", zap.String("loadinfo", loadinfo))
	return nil
}

func (sess *SnapshotReplicateSession) loadSnapshotDataIntoDataWarehouse() error {
	dumpFilePrefix := fmt.Sprintf("%s.%s.", sess.SourceDatabase, sess.SourceTable)
	if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, dumpFilePrefix, sess.OnSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func StartReplicateSnapshot(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	tableFQN string,
	tidbConfig *tidbsql.TiDBConfig,
	storageUri *url.URL,
) error {
	logger := log.L().With(zap.String("table", tableFQN))
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	session, err := NewSnapshotReplicateSession(ctx, dwConnector, tidbConfig, sourceDatabase, sourceTable, storageUri, logger)
	if err != nil {
		logger.Error("Failed to create snapshot replicate session", zap.Error(err), zap.String("tableFQN", tableFQN))
		return errors.Trace(err)
	}
	defer session.Close()
	if err := session.Run(); err != nil {
		logger.Error("Failed to load snapshot", zap.Error(err), zap.String("tableFQN", tableFQN))
		return errors.Trace(err)
	}
	logger.Info("Successfully load snapshot", zap.String("tableFQN", tableFQN))
	return nil
}
