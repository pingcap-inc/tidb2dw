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

const (
	DataWarehouseLoadConcurrency = 16
)

type SnapshotReplicateSession struct {
	TiDBConfig *tidbsql.TiDBConfig

	DataWarehousePool coreinterfaces.Connector
	TiDBPool          *sql.DB

	SourceDatabase string
	SourceTable    string

	StorageWorkspaceUri url.URL
	externalStorage     storage.ExternalStorage
	ParrallelLoad       bool

	ctx    context.Context
	logger *zap.Logger
}

func NewSnapshotReplicateSession(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	sourceDatabase, sourceTable string,
	storageUri *url.URL,
	parrallelLoad bool,
	logger *zap.Logger,
) (*SnapshotReplicateSession, error) {
	sess := &SnapshotReplicateSession{
		DataWarehousePool:   dwConnector,
		TiDBConfig:          tidbConfig,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		StorageWorkspaceUri: *storageUri,
		ParrallelLoad:       parrallelLoad,
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
}

func (sess *SnapshotReplicateSession) Run() error {
	switch sess.StorageWorkspaceUri.Scheme {
	case "s3", "gcs", "gs":
		if err := sess.DataWarehousePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
			return errors.Trace(err)
		}
		sess.logger.Info("Successfully copy table schema")
	default:
		return errors.Errorf("%s does not supprt data warehouse connector now...", sess.StorageWorkspaceUri.Scheme)
	}

	startTime := time.Now()
	if sess.ParrallelLoad {
		var snapshotFileSize int64
		var fileCount int64
		tableFQN := fmt.Sprintf("%s.%s", sess.SourceDatabase, sess.SourceTable)
		opt := &storage.WalkOption{ObjPrefix: fmt.Sprintf("%s.", tableFQN)}
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
		errFileCh := make(chan string, fileCount)
		blockCh := make(chan struct{}, DataWarehouseLoadConcurrency)
		var wg sync.WaitGroup
		if err := sess.externalStorage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
			if strings.HasSuffix(path, CSVFileExtension) {
				blockCh <- struct{}{}
				wg.Add(1)
				go func(path string, size int64) {
					defer func() {
						<-blockCh
						wg.Done()
					}()
					sess.logger.Info("Loading snapshot data into data warehouse", zap.String("path", path))
					if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, path); err != nil {
						sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err), zap.String("path", path))
						errFileCh <- path
					} else {
						sess.logger.Info("Successfully load snapshot data into data warehouse", zap.String("path", path))
						metrics.AddCounter(metrics.SnapshotLoadedSizeCounter, float64(size), tableFQN)
					}
				}(path, size)
			}
			return nil
		}); err != nil {
			return errors.Trace(err)
		}
		wg.Wait()
		close(errFileCh)
		close(blockCh)
		errFileList := make([]string, 0, len(errFileCh))
		for len(errFileCh) > 0 {
			errFileList = append(errFileList, <-errFileCh)
		}
		if len(errFileList) > 0 {
			return errors.Errorf("Failed to load snapshot data into data warehouse, error files: %v", errFileList)
		}
	} else {
		if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, fmt.Sprintf("%s.%s.*", sess.SourceDatabase, sess.SourceTable)); err != nil {
			sess.logger.Error("Failed to load snapshot data into data warehouse", zap.Error(err))
		}
	}
	endTime := time.Now()
	sess.logger.Info("Successfully load all snapshot data into data warehouse", zap.Duration("cost", endTime.Sub(startTime)))

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into data warehouse.
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err := sess.externalStorage.WriteFile(sess.ctx, fmt.Sprintf("snapshot/%s.%s.loadinfo", sess.SourceDatabase, sess.SourceTable), []byte(loadinfo)); err != nil {
		sess.logger.Error("Failed to upload loadinfo", zap.Error(err))
	}
	sess.logger.Info("Successfully upload loadinfo", zap.String("loadinfo", loadinfo))
	return nil
}

func StartReplicateSnapshot(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	tableFQN string,
	tidbConfig *tidbsql.TiDBConfig,
	storageUri *url.URL,
	parrallelLoad bool,
) error {
	logger := log.L().With(zap.String("table", tableFQN))
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	session, err := NewSnapshotReplicateSession(ctx, dwConnector, tidbConfig, sourceDatabase, sourceTable, storageUri, parrallelLoad, logger)
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
