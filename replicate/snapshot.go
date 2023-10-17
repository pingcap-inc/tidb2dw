package replicate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
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
		zap.String("storage", sess.StorageWorkspaceUri.Path))
	{
		db, err := tidbConfig.OpenDB()
		if err != nil {
			return nil, errors.Trace(err)
		}
		sess.TiDBPool = db
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
	if sess.TiDBPool != nil {
		sess.TiDBPool.Close()
	}
	if sess.DataWarehousePool != nil {
		sess.DataWarehousePool.Close()
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
	var snapshotFileSize int64
	var fileCount int64
	tableFQN := fmt.Sprintf("%s.%s", sess.SourceDatabase, sess.SourceTable)
	opt := &storage.WalkOption{}
	if err := sess.externalStorage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
		if strings.HasSuffix(path, CSVFileExtension) {
			snapshotFileSize += size
			fileCount++
		}
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	metrics.AddCounter(metrics.SnapshotTotalSizeCounter, float64(snapshotFileSize), tableFQN)
	errCh := make(chan error, fileCount)
	var wg sync.WaitGroup
	if err := sess.externalStorage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
		if strings.HasSuffix(path, CSVFileExtension) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sess.loadSnapshotDataIntoDataWarehouse(path); err != nil {
					sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err), zap.String("path", path))
					errCh <- errors.Annotate(err, "Failed to load snapshot data into data warehouse")
					return
				}
				metrics.AddCounter(metrics.SnapshotLoadedSizeCounter, float64(size), tableFQN)
			}()
		}
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	wg.Wait()
	if len(errCh) > 0 {
		return errors.Trace(<-errCh)
	}
	endTime := time.Now()
	sess.logger.Info("Successfully load snapshot data into data warehouse", zap.Int64("size", snapshotFileSize), zap.Duration("cost", endTime.Sub(startTime)))

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into data warehouse.
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err := sess.externalStorage.WriteFile(sess.ctx, "loadinfo", []byte(loadinfo)); err != nil {
		sess.logger.Error("Failed to upload loadinfo", zap.Error(err))
	}
	sess.logger.Info("Successfully upload loadinfo", zap.String("loadinfo", loadinfo))
	return nil
}

func (sess *SnapshotReplicateSession) loadSnapshotDataIntoDataWarehouse(filePath string) error {
	if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, filePath); err != nil {
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
		logger.Error("Failed to create snapshot replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	defer session.Close()
	if err := session.Run(); err != nil {
		logger.Error("Failed to load snapshot", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
