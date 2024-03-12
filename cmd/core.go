package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/dumpling"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/dumpling/export"
	tiflow_config "github.com/pingcap/tiflow/pkg/config"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

type RunMode enumflag.Flag

const (
	RunModeFull RunMode = iota
	RunModeSnapshotOnly
	RunModeIncrementalOnly
	RunModeCloud
)

var RunModeIds = map[RunMode][]string{
	RunModeFull:            {"full"},
	RunModeSnapshotOnly:    {"snapshot-only"},
	RunModeIncrementalOnly: {"incremental-only"},
	RunModeCloud:           {"cloud"},
}

// o => create changefeed =>   dump snapshot   => load snapshot => incremental load
//
//	^                     ^                    ^ 				 ^
//	|			          |				       |				 |
//	+------ init ---------+ changefeed created + snapshot dumped + snapshot loaded --
type Stage string

const (
	StageInit              Stage = "init"
	StageChangefeedCreated Stage = "changefeed-created"
	StageSnapshotDumped    Stage = "snapshot-dumped"
	StageSnapshotLoaded    Stage = "snapshot-loaded"
)

var DumplingCsvOutputDialectMap = map[string]export.CSVDialect{
	"":          export.CSVDialectDefault,
	"default":   export.CSVDialectDefault,
	"bigquery":  export.CSVDialectBigQuery,
	"snowflake": export.CSVDialectSnowflake,
	"redshift":  export.CSVDialectRedshift,
}

var CdcCsvBinaryEncodingMethodMap = map[string]string{
	"":          tiflow_config.BinaryEncodingHex,
	"default":   tiflow_config.BinaryEncodingHex,
	"bigquery":  tiflow_config.BinaryEncodingBase64,
	"snowflake": tiflow_config.BinaryEncodingHex,
	"redshift":  tiflow_config.BinaryEncodingHex,
}

func isChangeFeedCreated(ctx context.Context, storage storage.ExternalStorage) (bool, error) {
	return storage.FileExists(ctx, "increment/metadata")
}

func isDumplingWalkerCreated(ctx context.Context, storage storage.ExternalStorage) (bool, error) {
	return storage.FileExists(ctx, "snapshot/metadata")
}

func isSnapshotLoaded(ctx context.Context, storage storage.ExternalStorage, tableName string) (bool, error) {
	return storage.FileExists(ctx, fmt.Sprintf("snapshot/%s.loadinfo", tableName))
}

func getGCSURIWithCredentials(storagePath string, credentialsFilePath string) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}

	if uri.Scheme != "gcs" && uri.Scheme != "gs" {
		return nil, errors.New("Not a gcs storage")
	}

	// bigquery does not support gcs scheme
	if uri.Scheme == "gcs" {
		uri.Scheme = "gs"
	}

	// append credentials file path to query string
	if credentialsFilePath != "" {
		values := url.Values{}
		values.Add("credentials-file", credentialsFilePath)
		uri.RawQuery = values.Encode()
	}
	return uri, nil
}

func getS3URIWithCredentials(storagePath string, cred *credentials.Value) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to parse workspace path")
	}

	if uri.Scheme != "s3" {
		return nil, errors.New("Not a s3 storage")
	}

	// append credentials to query string
	values := url.Values{}
	values.Add("access-key", cred.AccessKeyID)
	values.Add("secret-access-key", cred.SecretAccessKey)
	if cred.SessionToken != "" {
		values.Add("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func genSnapshotAndIncrementURIs(storageURI *url.URL) (*url.URL, *url.URL, error) {
	// create snapshot and increment uri from storage uri, append snapshot and increment path to path
	snapshotURI := *storageURI
	incrementURI := *storageURI

	var err error

	snapshotURI.Path, err = url.JoinPath(storageURI.Path, "snapshot")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	incrementURI.Path, err = url.JoinPath(storageURI.Path, "increment")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	return &snapshotURI, &incrementURI, nil
}

func resolveAWSCredential(storagePath string) (*credentials.Value, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if uri.Scheme == "s3" {
		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		return &credValue, err
	}
	return nil, errors.New("Not a s3 storage")
}

func Export(
	ctx context.Context,
	tidbConfig *tidbsql.TiDBConfig,
	tables []string,
	storageURI *url.URL,
	snapshotURI *url.URL,
	incrementURI *url.URL,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int,
	csvOutputDialect string,
	mode RunMode,
) error {
	storage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return errors.Trace(err)
	}

	startTSO := uint64(0)
	if mode == RunModeFull {
		startTSO, err = tidbsql.GetCurrentTSO(tidbConfig)
		if err != nil {
			return errors.Annotate(err, "Failed to get current TSO")
		}
	}

	if mode != RunModeSnapshotOnly && mode != RunModeCloud {
		created, err := isChangeFeedCreated(ctx, storage)
		if err != nil {
			return errors.Trace(err)
		}
		if !created {
			cdcConnector, err := cdc.NewCDCConnector(
				cdcHost, cdcPort, tables, startTSO, incrementURI, cdcFlushInterval, cdcFileSize,
				CdcCsvBinaryEncodingMethodMap[csvOutputDialect],
			)
			if err != nil {
				return errors.Trace(err)
			}
			if err = cdcConnector.CreateChangefeed(); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if mode != RunModeIncrementalOnly && mode != RunModeCloud {
		created, err := isDumplingWalkerCreated(ctx, storage)
		if err != nil {
			return errors.Trace(err)
		}
		if !created {
			onSnapshotDumpProgress := func(dumpedRows, totalRows int64) {
				log.Info("Snapshot dump progress", zap.Int64("dumpedRows", dumpedRows), zap.Int64("estimatedTotalRows", totalRows))
			}
			if err := dumpling.RunDump(
				tidbConfig, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), tables,
				DumplingCsvOutputDialectMap[csvOutputDialect], onSnapshotDumpProgress,
			); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func Replicate(
	tidbConfig *tidbsql.TiDBConfig,
	tables []string,
	storageURI *url.URL,
	snapshotURI *url.URL,
	incrementURI *url.URL,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int,
	snapConnectorMap map[string]coreinterfaces.Connector,
	increConnectorMap map[string]coreinterfaces.Connector,
	csvOutputDialect string,
	parrallelLoad bool,
	mode RunMode,
) error {
	ctx := context.Background()
	metrics.TableNumGauge.Add(float64(len(tables)))
	if err := Export(ctx, tidbConfig, tables, storageURI, snapshotURI, incrementURI,
		snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize, csvOutputDialect, mode); err != nil {
		return errors.Trace(err)
	}

	storage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return errors.Trace(err)
	}
	onError := func(table string, err error) {
		apiservice.GlobalInstance.APIInfo.SetTableFatalError(table, err)
		metrics.AddCounter(metrics.ErrorCounter, 1, table)
	}

	var wg sync.WaitGroup
	for _, table := range tables {
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			if mode != RunModeIncrementalOnly {
				loaded, err := isSnapshotLoaded(ctx, storage, table)
				if err != nil {
					onError(table, err)
					return
				}
				if !loaded {
					apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageLoadingSnapshot)
					if err := replicate.StartReplicateSnapshot(ctx, snapConnectorMap[table], table, tidbConfig, snapshotURI, parrallelLoad); err != nil {
						onError(table, err)
						return
					}
				}
			}
			if mode != RunModeSnapshotOnly {
				apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageLoadingIncremental)
				if err := replicate.StartReplicateIncrement(ctx, increConnectorMap[table], table, incrementURI, cdcFlushInterval/5); err != nil {
					onError(table, err)
					return
				}
			}
			apiservice.GlobalInstance.APIInfo.SetTableStage(table, apiservice.TableStageFinished)
		}(table)
	}

	wg.Wait()
	return nil
}

func runWithServer(startServer bool, addr string, body func()) {
	if !startServer {
		body()
		return
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Start API service failed", zap.Error(err))
		return
	}

	log.Info("API service started", zap.String("address", addr))

	go func() {
		body()
	}()

	apiservice.GlobalInstance.Serve(l)
}
