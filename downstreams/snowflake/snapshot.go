package snowflake

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
	"github.com/pingcap-inc/tidb2dw/snowsql"
	"github.com/pingcap-inc/tidb2dw/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type SnapshotReplicateSession struct {
	SFConfig            *snowsql.SnowflakeConfig
	TiDBConfig          *tidbsql.TiDBConfig
	TableFQN            string
	SnapshotConcurrency int
	ResolvedS3Region    string
	ResolvedTSO         string // Available after buildDumper()

	AWSCredential *credentials.Value // The resolved credential from current env
	SnowflakePool *snowsql.SnowflakeConnector
	TiDBPool      *sql.DB

	SourceDatabase string
	SourceTable    string
	StartTSO       string

	OnSnapshotDumpProgress func(dumpedRows, totalRows int64)
	OnSnapshotLoadProgress func(loadedRows int64)

	StorageWorkspaceUri url.URL
	StorageSchema       string
}

func NewSnapshotReplicateSession(
	sfConfig *snowsql.SnowflakeConfig,
	tidbConfig *tidbsql.TiDBConfig,
	tableFQN string,
	snapshotConcurrency int,
	s3StoragePath string,
	startTSO string,
	credential *credentials.Value) (*SnapshotReplicateSession, error) {
	sess := &SnapshotReplicateSession{
		SFConfig:            sfConfig,
		TiDBConfig:          tidbConfig,
		TableFQN:            tableFQN,
		SnapshotConcurrency: snapshotConcurrency,
		StartTSO:            startTSO,
	}
	workUri, err := url.Parse(s3StoragePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}
	sess.StorageWorkspaceUri = *workUri
	{
		parts := strings.SplitN(tableFQN, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("table must be a full-qualified name like mydb.mytable")
		}
		sess.SourceDatabase = parts[0]
		sess.SourceTable = parts[1]
	}
	log.Info("Creating replicate session",
		zap.String("storage", sess.StorageWorkspaceUri.String()),
		zap.String("source", tableFQN))
	sess.AWSCredential = credential
	{
		// Parse s3StoragePath like s3://snowflake-test/dump20230601
		parsed, err := url.Parse(s3StoragePath)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to parse --storage value")
		}
		sess.StorageSchema = parsed.Scheme
		switch sess.StorageSchema {
		case "s3":
			awsSession, err := session.NewSessionWithOptions(session.Options{
				SharedConfigState: session.SharedConfigEnable,
			})
			if err != nil {
				return nil, errors.Annotate(err, "Failed to establish AWS session")
			}

			bucket := parsed.Host
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
		switch sess.StorageSchema {
		case "s3":
			db, err := snowsql.OpenSnowflake(sfConfig)
			if err != nil {
				return nil, errors.Trace(err)
			}
			connector, err := snowsql.NewSnowflakeConnector(
				db,
				fmt.Sprintf("snapshot_stage_%s", sess.SourceTable),
				&sess.StorageWorkspaceUri,
				sess.AWSCredential,
			)
			if err != nil {
				return nil, errors.Annotate(err, "Failed to create Snowflake connector")
			}
			sess.SnowflakePool = connector
		case "gcs":
			sess.SnowflakePool = nil
			log.Info("Skip connecting to snowflake. GCS does not support snowflake connector now...")
		}

	}
	{
		db, err := tidbsql.OpenTiDB(tidbConfig)
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
	// no connector for gcs now
	if sess.SnowflakePool != nil {
		sess.SnowflakePool.Close()
	}
	sess.TiDBPool.Close()
}

func (sess *SnapshotReplicateSession) Run() error {
	dumper, err := sess.buildDumper()
	if err != nil {
		return errors.Trace(err)
	}
	switch sess.StorageSchema {
	case "s3":
		if err = sess.SnowflakePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
			return errors.Trace(err)
		}
	case "gcs":
		log.Info("Skip snowflake table schema copy. GCS does not supprt snowflake connector now...")
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
	log.Info("Successfully dumped table from TiDB, starting to load into Snowflake", zap.Any("status", status))

	if sess.StorageSchema == "gcs" {
		log.Info("Skip loading. GCS does not supprt snowflake connector now...")
		return nil
	}

	startTime := time.Now()
	if err = sess.loadSnapshotDataIntoSnowflake(); err != nil {
		return errors.Annotate(err, "Failed to load snapshot data into Snowflake")
	}
	endTime := time.Now()

	// Write load info to workspace to record the status of load,
	// loadinfo exists means the data has been all loaded into snowflake.
	ctx := context.Background()
	storage, err := putil.GetExternalStorageFromURI(ctx, sess.StorageWorkspaceUri.String())
	if err != nil {
		log.Error("Failed to get external storage", zap.Error(err))
	}
	loadinfo := fmt.Sprintf("Copy to snowflake start time: %s\nCopy to snowflake end time: %s", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
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
	tables, err := export.GetConfTables([]string{sess.TableFQN})
	if err != nil {
		return nil, errors.Trace(err) // Should not happen
	}
	conf.Tables = tables

	return conf, nil
}

func (sess *SnapshotReplicateSession) loadSnapshotDataIntoSnowflake() error {
	workspacePrefix := strings.TrimPrefix(sess.StorageWorkspaceUri.Path, "/")
	dumpFilePrefix := fmt.Sprintf("%s/%s.%s.", workspacePrefix, sess.SourceDatabase, sess.SourceTable)
	if err := sess.SnowflakePool.LoadSnapshot(sess.SourceTable, dumpFilePrefix, sess.OnSnapshotLoadProgress); err != nil {
		return errors.Trace(err)
	}
	// TODO: remove dump files
	return nil
}

func StartReplicateSnapshot(
	sfConfig *snowsql.SnowflakeConfig,
	tidbConfig *tidbsql.TiDBConfig,
	tableFQN string,
	snapshotConcurrency int,
	s3StoragePath string,
	startTSO string,
	credential *credentials.Value) error {
	session, err := NewSnapshotReplicateSession(sfConfig, tidbConfig, tableFQN, snapshotConcurrency, s3StoragePath, startTSO, credential)
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()

	if err = session.Run(); err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully replicated snapshot from TiDB to Snowflake")

	return nil
}
