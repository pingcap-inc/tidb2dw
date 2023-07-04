package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
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
	"go.uber.org/zap"
)

type SnapshotReplicateSession struct {
	SFConfig            *snowsql.SnowflakeConfig
	TiDBConfig          *tidbsql.TiDBConfig
	TableFQN            string
	SnapshotConcurrency int
	ResolvedS3Region    string
	ResolvedTSO         string // Available after buildDumper()

	AWSSession    *session.Session
	AWSCredential credentials.Value // The resolved credential from current env
	SnowflakePool *snowsql.SnowflakeConnector
	TiDBPool      *sql.DB

	SourceDatabase string
	SourceTable    string
	StartTSO       string

	OnSnapshotDumpProgress func(dumpedRows, totalRows int64)
	OnSnapshotLoadProgress func(loadedRows int64)

	StorageWorkspaceUri url.URL
}

func NewSnapshotReplicateSession(
	sfConfig *snowsql.SnowflakeConfig,
	tidbConfig *tidbsql.TiDBConfig,
	tableFQN string,
	snapshotConcurrency int,
	s3StoragePath string,
	startTSO string) (*SnapshotReplicateSession, error) {
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
	{
		awsSession, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, errors.Annotate(err, "Failed to establish AWS session")
		}
		sess.AWSSession = awsSession

		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		if err != nil {
			return nil, errors.Annotate(err, "Failed to resolve AWS credential")
		}
		sess.AWSCredential = credValue
	}
	{
		// Parse s3StoragePath like s3://wenxuan-snowflake-test/dump20230601
		parsed, err := url.Parse(s3StoragePath)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to parse --storage value")
		}
		if parsed.Scheme != "s3" {
			return nil, errors.Errorf("storage must be like s3://...")
		}

		bucket := parsed.Host
		log.Debug("Resolving storage region")
		s3Region, err := s3manager.GetBucketRegion(context.Background(), sess.AWSSession, bucket, "us-west-2")
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				return nil, fmt.Errorf("unable to find bucket %s's region not found", bucket)
			}
			return nil, errors.Annotate(err, "Failed to resolve --storage region")
		}
		sess.ResolvedS3Region = s3Region
		log.Info("Resolved storage region", zap.String("region", s3Region))
	}
	{
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
	sess.SnowflakePool.Close()
	sess.TiDBPool.Close()
}

func (sess *SnapshotReplicateSession) Run() error {
	dumper, err := sess.buildDumper()
	if err != nil {
		return errors.Trace(err)
	}

	if err = sess.SnowflakePool.CopyTableSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool); err != nil {
		return errors.Trace(err)
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

	if err = sess.loadSnapshotDataIntoSnowflake(); err != nil {
		return errors.Annotate(err, "Failed to load snapshot data into Snowflake")
	}
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
	conf.S3.Region = sess.ResolvedS3Region
	conf.Snapshot = sess.StartTSO

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
	startTSO string) error {
	session, err := NewSnapshotReplicateSession(sfConfig, tidbConfig, tableFQN, snapshotConcurrency, s3StoragePath, startTSO)
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
