package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/dumpling"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
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

func checkStage(storage storage.ExternalStorage) (Stage, error) {
	stage := StageInit
	ctx := context.Background()
	if exist, err := storage.FileExists(ctx, "increment/metadata"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check increment metadata")
	} else {
		stage = StageChangefeedCreated
	}
	if exist, err := storage.FileExists(ctx, "snapshot/metadata"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check snapshot metadata")
	} else {
		stage = StageSnapshotDumped
	}
	if exist, err := storage.FileExists(ctx, "snapshot/loadinfo"); err != nil || !exist {
		return stage, errors.Wrap(err, "Failed to check snapshot loadinfo")
	} else {
		stage = StageSnapshotLoaded
	}
	return stage, nil
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
	values := url.Values{}
	values.Add("credentials-file", credentialsFilePath)
	uri.RawQuery = values.Encode()
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

	// append credentials file path to query string
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

func genURI(storagePath string) (*url.URL, *url.URL, error) {
	snapStoragePath, err := url.JoinPath(storagePath, "snapshot")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	snapshotURI, err := url.Parse(snapStoragePath)
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to parse workspace path")
	}
	increStoragePath, err := url.JoinPath(storagePath, "increment")
	if err != nil {
		return nil, nil, errors.Annotate(err, "Failed to join workspace path")
	}
	incrementURI, err := url.Parse(increStoragePath)
	if err != nil {
		return snapshotURI, nil, errors.Annotate(err, "Failed to parse workspace path")
	}
	return snapshotURI, incrementURI, nil
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

func Replicate(
	tidbConfig *tidbsql.TiDBConfig,
	tableName string,
	storageURI *url.URL,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int64,
	credValue credentials.Value,
	snapConnector coreinterfaces.Connector,
	increConnector coreinterfaces.Connector,
	timezone string,
	mode RunMode,
) error {
	ctx := context.Background()
	storage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return errors.Trace(err)
	}
	stage, err := checkStage(storage)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Start Replicate", zap.String("stage", string(stage)), zap.String("mode", RunModeIds[mode][0]))

	startTSO := uint64(0)
	if mode == RunModeFull {
		startTSO, err = tidbsql.GetCurrentTSO(tidbConfig)
		if err != nil {
			return errors.Annotate(err, "Failed to get current TSO")
		}
	}
	snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(storageURI)
	if err != nil {
		return errors.Trace(err)
	}

	onSnapshotDumpProgress := func(dumpedRows, totalRows int64) {
		log.Info("Snapshot dump progress", zap.Int64("dumpedRows", dumpedRows), zap.Int64("estimatedTotalRows", totalRows))
	}

	switch stage {
	case StageInit:
		if mode != RunModeSnapshotOnly && mode != RunModeCloud {
			cdcConnector, err := cdc.NewCDCConnector(cdcHost, cdcPort, tableName, startTSO, incrementURI, cdcFlushInterval, cdcFileSize)
			if err != nil {
				return errors.Trace(err)
			}
			if err = cdcConnector.CreateChangefeed(); err != nil {
				return errors.Trace(err)
			}
		}
		fallthrough
	case StageChangefeedCreated:
		if mode != RunModeIncrementalOnly && mode != RunModeCloud {
			if err := dumpling.RunDump(tidbConfig, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), []string{tableName}, onSnapshotDumpProgress); err != nil {
				return errors.Trace(err)
			}
		}
		fallthrough
	case StageSnapshotDumped:
		if mode != RunModeIncrementalOnly {
			if err = replicate.StartReplicateSnapshot(snapConnector, tidbConfig, tableName, snapshotURI); err != nil {
				return errors.Annotate(err, "Failed to replicate snapshot")
			}
		}
		fallthrough
	case StageSnapshotLoaded:
		if mode != RunModeSnapshotOnly {
			if err = replicate.StartReplicateIncrement(increConnector, incrementURI, cdcFlushInterval/5, timezone); err != nil {
				return errors.Annotate(err, "Failed to replicate incremental")
			}
		}

	}
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
