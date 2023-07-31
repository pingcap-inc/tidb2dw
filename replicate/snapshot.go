package replicate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type SnapshotReplicateSession struct {
	TiDBConfig          *tidbsql.TiDBConfig
	SnapshotConcurrency int
	ResolvedS3Region    string
	ResolvedTSO         string // Available after buildDumper()

	AWSCredential     *credentials.Value // The resolved credential from current env
	DataWarehousePool coreinterfaces.Connector
	TiDBPool          *sql.DB

	SourceDatabase string
	SourceTable    string
	StartTSO       string

	OnSnapshotDumpProgress func(dumpedRows, totalRows int64)
	OnSnapshotLoadProgress func(loadedRows int64)

	StorageWorkspaceUri url.URL
	StorageSchema       string
}

func NewSnapshotReplicateSession(
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	sourceDatabase, sourceTable string,
	snapshotConcurrency int,
	s3StoragePath string,
	startTSO string,
	credential *credentials.Value) (*SnapshotReplicateSession, error) {
	sess := &SnapshotReplicateSession{
		TiDBConfig:          tidbConfig,
		SourceDatabase:      sourceDatabase,
		SourceTable:         sourceTable,
		SnapshotConcurrency: snapshotConcurrency,
		StartTSO:            startTSO,
	}
	sess.DataWarehousePool = dwConnector
	workUri, err := url.Parse(s3StoragePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}
	sess.StorageWorkspaceUri = *workUri
	log.Info("Creating replicate session",
		zap.String("storage", sess.StorageWorkspaceUri.String()),
		zap.String("source", fmt.Sprintf("%s.%s", sourceDatabase, sourceTable)))
	sess.AWSCredential = credential
	{
		sess.StorageSchema = workUri.Scheme
		switch sess.StorageSchema {
		case "s3":
			awsSession, err := session.NewSessionWithOptions(session.Options{
				SharedConfigState: session.SharedConfigEnable,
			})
			if err != nil {
				return nil, errors.Annotate(err, "Failed to establish AWS session")
			}

			bucket := workUri.Host
			log.Debug("Resolving storage region")
			s3Region, err := s3manager.GetBucketRegion(context.Background(), awsSession, bucket, "us-west-2")
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
					return nil, fmt.Errorf("unable to find bucket %s's region not found", bucket)
				}
				return nil, errors.Annotate(err, "Failed to resolve --storage region")
			}
			sess.ResolvedS3Region = s3Region
			log.Info("Resolved storage region", zap.String("region", s3Region))
		case "gcs":
		default:
			return nil, errors.Errorf("storage must be like s3://... or gcs://...")
		}
	}
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
			log.Info("Snapshot load progress", zap.Int64("loadedRows", loadedRows))
		}
		sess.OnSnapshotDumpProgress = func(dumpedRows, totalRows int64) {
			log.Info("Snapshot dump progress", zap.Int64("dumpedRows", dumpedRows), zap.Int64("estimatedTotalRows", totalRows))
		}
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
	dumper, err := sess.buildDumper()
	if err != nil {
		return errors.Trace(err)
	}
	switch sess.StorageSchema {
	case "s3":
		if err = sess.DataWarehousePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
			return errors.Trace(err)
		}
	case "gcs":
		log.Info("Skip table schema copy. GCS does not supprt data warehouse connector now...")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	dumpFinished := make(chan struct{})

	go func() {
		// This is a goroutine to monitor the dump progress.
		defer wg.Done()

		if sess.OnSnapshotDumpProgress == nil {
			return
		}

		checkInterval := 10 * time.Second
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-dumpFinished:
				return
			case <-ticker.C:
				status := dumper.GetStatus()
				sess.OnSnapshotDumpProgress(int64(status.FinishedRows), int64(status.EstimateTotalRows))
			}
		}
	}()

	err = dumper.Dump()
	dumpFinished <- struct{}{}

	wg.Wait()

	_ = dumper.Close()
	if err != nil {
		return errors.Annotate(err, "Failed to dump table from TiDB")
	}
	status := dumper.GetStatus()
	log.Info("Successfully dumped table from TiDB, starting to load into data warehouse", zap.Any("status", status))

	if sess.StorageSchema == "gcs" {
		log.Info("Skip loading. GCS does not supprt data warehouse connector now...")
		return nil
	}

	startTime := time.Now()
	if err = sess.loadSnapshotDataIntoDataWarehouse(); err != nil {
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
	loadinfo := fmt.Sprintf("Copy to data warehouse start time: %s\nCopy to data warehouse end time: %s", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	if err = storage.WriteFile(ctx, "loadinfo", []byte(loadinfo)); err != nil {
		log.Error("Failed to upload loadinfo", zap.Error(err))
	}
	log.Info("Successfully upload loadinfo", zap.String("loadinfo", loadinfo))
	return nil
}

func (sess *SnapshotReplicateSession) buildDumper() (*export.Dumper, error) {
	conf, err := sess.buildDumperConfig()
	if err != nil {
		return nil, errors.Annotate(err, "Failed to build dumpling config")
	}
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create dumpling instance")
	}

	sess.ResolvedTSO = conf.Snapshot
	if len(sess.ResolvedTSO) == 0 {
		return nil, errors.Errorf("Snapshot is not available")
	}
	// FIXME: This might cause a bug, because the underlying is a pool?
	_, err = sess.TiDBPool.ExecContext(context.Background(), "SET SESSION tidb_snapshot = ?", conf.Snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("Using snapshot", zap.String("snapshot", sess.ResolvedTSO))

	return dumper, nil
}

func (sess *SnapshotReplicateSession) buildDumperConfig() (*export.Config, error) {
	conf := export.DefaultConfig()
	conf.Logger = log.L()
	conf.User = sess.TiDBConfig.User
	conf.Password = sess.TiDBConfig.Pass
	conf.Host = sess.TiDBConfig.Host
	conf.Port = sess.TiDBConfig.Port
	conf.Threads = sess.SnapshotConcurrency
	conf.NoHeader = true
	conf.FileType = "csv"
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.EscapeBackslash = true
	conf.TransactionalConsistency = true
	conf.OutputDirPath = sess.StorageWorkspaceUri.String()

	conf.Snapshot = sess.StartTSO

	switch sess.StorageSchema {
	case "s3":
		conf.S3.Region = sess.ResolvedS3Region
	case "gcs":
		credFile, found := syscall.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			log.Error("Failed to resolve AWS credential")
		}
		conf.GCS.CredentialsFile = credFile
	}

	filesize, err := export.ParseFileSize("5GiB")
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.FileSize = filesize

	conf.SpecifiedTables = true
	tables, err := export.GetConfTables([]string{fmt.Sprintf("%s.%s", sess.SourceDatabase, sess.SourceTable)})
	if err != nil {
		return nil, errors.Trace(err) // Should not happen
	}
	conf.Tables = tables

	return conf, nil
}

func (sess *SnapshotReplicateSession) loadSnapshotDataIntoDataWarehouse() error {
	workspacePrefix := strings.TrimPrefix(sess.StorageWorkspaceUri.Path, "/")
	dumpFilePrefix := fmt.Sprintf("%s/%s.%s.", workspacePrefix, sess.SourceDatabase, sess.SourceTable)
	if err := sess.DataWarehousePool.LoadSnapshot(sess.SourceTable, dumpFilePrefix, sess.OnSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	// TODO: remove dump files
	return nil
}

func StartReplicateSnapshot(
	dwConnector coreinterfaces.Connector,
	tidbConfig *tidbsql.TiDBConfig,
	sourceDatabase, sourceTable string,
	snapshotConcurrency int,
	s3StoragePath string,
	startTSO string,
	credential *credentials.Value) error {
	session, err := NewSnapshotReplicateSession(dwConnector, tidbConfig, sourceDatabase, sourceTable, snapshotConcurrency, s3StoragePath, startTSO, credential)
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()

	if err = session.Run(); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully replicated snapshot from TiDB to Data Warehouse")

	return nil
}
