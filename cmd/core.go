package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/dumpling"
	"github.com/pingcap-inc/tidb2dw/pkg/icebergconsumer"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	ticdcconfig "github.com/pingcap/ticdc/pkg/config"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
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
	"":          ticdcconfig.BinaryEncodingHex,
	"default":   ticdcconfig.BinaryEncodingHex,
	"bigquery":  ticdcconfig.BinaryEncodingBase64,
	"snowflake": ticdcconfig.BinaryEncodingHex,
	"redshift":  ticdcconfig.BinaryEncodingHex,
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
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
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

	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, storageURI.String())
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

func ReplicateIceberg(
	tables []string,
	stagingURI *url.URL,
	sourceOpts sourceOptions,
	appliers map[string]coreinterfaces.RowApplier,
	mode RunMode,
) error {
	if len(appliers) == 0 {
		return errors.New("iceberg source format requires at least one table")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := icebergconsumer.NewConfig(sourceOpts.icebergSourceURI, sourceOpts.icebergPollInterval)
	if err != nil {
		return errors.Trace(err)
	}

	icebergCfg := sinkiceberg.NewConfig()
	if err := icebergCfg.Apply(ctx, cfg.SourceURI, nil); err != nil {
		return errors.Trace(err)
	}

	sourceStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, icebergCfg.WarehouseURI)
	if err != nil {
		return errors.Trace(err)
	}
	stagingStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, stagingURI.String())
	if err != nil {
		return errors.Trace(err)
	}

	metrics.TableNumGauge.Add(float64(len(appliers)))
	tableFilter := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		tableFilter[table] = struct{}{}
	}

	runOnce := func() error {
		discoveredTables, err := sinkiceberg.ListHadoopTables(ctx, icebergCfg, sourceStorage)
		if err != nil {
			return errors.Trace(err)
		}

		for _, table := range discoveredTables {
			tableFQN := fmt.Sprintf("%s.%s", table.SchemaName, table.TableName)
			if len(tableFilter) > 0 {
				if _, ok := tableFilter[tableFQN]; !ok {
					continue
				}
			}

			applier := appliers[tableFQN]
			if applier == nil {
				continue
			}

			if err := processIcebergTable(
				ctx,
				sourceOpts.icebergSourceURI,
				stagingStorage,
				sourceStorage,
				icebergCfg,
				table,
				applier,
				mode,
			); err != nil {
				return errors.Annotatef(err, "failed to replicate iceberg table %s", tableFQN)
			}
		}
		return nil
	}

	if err := runOnce(); err != nil {
		return errors.Trace(err)
	}
	if mode == RunModeSnapshotOnly {
		return nil
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err := runOnce(); err != nil {
			return errors.Trace(err)
		}
	}
}

func processIcebergTable(
	ctx context.Context,
	sourceID string,
	checkpointStorage storage.ExternalStorage,
	sourceStorage storage.ExternalStorage,
	icebergCfg *sinkiceberg.Config,
	table sinkiceberg.TableIdentifier,
	applier coreinterfaces.RowApplier,
	mode RunMode,
) error {
	tableFQN := fmt.Sprintf("%s.%s", table.SchemaName, table.TableName)
	checkpoint, checkpointExists, err := icebergconsumer.LoadCheckpoint(checkpointStorage, sourceID, table.SchemaName, table.TableName)
	if err != nil {
		return errors.Trace(err)
	}

	if !checkpointExists && mode == RunModeIncrementalOnly {
		latestVersion, err := sinkiceberg.LoadTableVersion(
			ctx,
			icebergCfg,
			sourceStorage,
			table.SchemaName,
			table.TableName,
			table.LatestMetadataVersion,
		)
		if err != nil {
			return errors.Trace(err)
		}

		tableDef, err := icebergconsumer.BuildTableDefinition(latestVersion)
		if err != nil {
			return errors.Trace(err)
		}
		if err := applier.CreateTableFromDefinition(tableDef); err != nil {
			return errors.Trace(err)
		}

		checkpoint = icebergconsumer.Checkpoint{
			SourceID:            sourceID,
			Schema:              table.SchemaName,
			Table:               table.TableName,
			LastMetadataVersion: table.LatestMetadataVersion,
			DDLApplied:          true,
			LastDataFile:        "",
		}
		if err := icebergconsumer.SaveCheckpoint(checkpointStorage, checkpoint); err != nil {
			return errors.Trace(err)
		}
		metrics.IcebergMetadataVersionGauge.WithLabelValues(tableFQN).Set(float64(table.LatestMetadataVersion))
		return nil
	}

	startVersion := 1
	if checkpointExists && checkpoint.LastMetadataVersion > 0 {
		startVersion = checkpoint.LastMetadataVersion
	}

	for versionNum := startVersion; versionNum <= table.LatestMetadataVersion; versionNum++ {
		version, err := sinkiceberg.LoadTableVersion(
			ctx,
			icebergCfg,
			sourceStorage,
			table.SchemaName,
			table.TableName,
			versionNum,
		)
		if err != nil {
			return errors.Trace(err)
		}

		if checkpointExists && versionNum == checkpoint.LastMetadataVersion && checkpoint.DDLApplied &&
			icebergVersionCompleted(checkpoint.LastDataFile, version.DataFiles) {
			continue
		}
		if !checkpointExists || versionNum != checkpoint.LastMetadataVersion {
			checkpoint = icebergconsumer.Checkpoint{
				SourceID:            sourceID,
				Schema:              table.SchemaName,
				Table:               table.TableName,
				LastMetadataVersion: versionNum,
				DDLApplied:          false,
				LastDataFile:        "",
			}
			checkpointExists = true
		}

		if !checkpoint.DDLApplied {
			if err := applyIcebergVersionDDL(ctx, sourceStorage, icebergCfg, version, applier); err != nil {
				return errors.Trace(err)
			}
			checkpoint.DDLApplied = true
			if err := icebergconsumer.SaveCheckpoint(checkpointStorage, checkpoint); err != nil {
				return errors.Trace(err)
			}
			metrics.IcebergMetadataVersionGauge.WithLabelValues(tableFQN).Set(float64(versionNum))
		}

		tableDef, err := icebergconsumer.BuildTableDefinition(version)
		if err != nil {
			return errors.Trace(err)
		}

		startFileIdx := 0
		if checkpoint.LastDataFile != "" {
			for i, dataFile := range version.DataFiles {
				if dataFile == checkpoint.LastDataFile {
					startFileIdx = i + 1
					break
				}
			}
		}

		for _, dataFile := range version.DataFiles[startFileIdx:] {
			rows, err := sinkiceberg.DecodeParquetFile(ctx, sourceStorage, dataFile)
			if err != nil {
				return errors.Trace(err)
			}
			changes, err := icebergconsumer.ToRowChanges(rows)
			if err != nil {
				return errors.Trace(err)
			}
			if len(changes) > 0 {
				if err := applier.ApplyRows(tableDef, changes); err != nil {
					return errors.Trace(err)
				}
			}
			checkpoint.LastDataFile = dataFile
			if err := icebergconsumer.SaveCheckpoint(checkpointStorage, checkpoint); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func applyIcebergVersionDDL(
	ctx context.Context,
	sourceStorage storage.ExternalStorage,
	icebergCfg *sinkiceberg.Config,
	version *sinkiceberg.TableVersion,
	applier coreinterfaces.RowApplier,
) error {
	if version.MetadataVersion == 1 {
		tableDef, err := icebergconsumer.BuildTableDefinition(version)
		if err != nil {
			return errors.Trace(err)
		}
		return applier.CreateTableFromDefinition(tableDef)
	}

	prevVersion, err := sinkiceberg.LoadTableVersion(
		ctx,
		icebergCfg,
		sourceStorage,
		version.SchemaName,
		version.TableName,
		version.MetadataVersion-1,
	)
	if err != nil {
		return errors.Trace(err)
	}

	ddlSteps, err := icebergconsumer.BuildDDLDefinitions(prevVersion, version)
	if err != nil {
		return errors.Trace(err)
	}
	for _, step := range ddlSteps {
		if step.Type == byte(timodel.ActionCreateTable) {
			if err := applier.CreateTableFromDefinition(step); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if err := applier.ExecDDL(step); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func icebergVersionCompleted(lastDataFile string, dataFiles []string) bool {
	if len(dataFiles) == 0 {
		return true
	}
	return lastDataFile != "" && lastDataFile == dataFiles[len(dataFiles)-1]
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
