package cmd

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/dumpling"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

type RunMode enumflag.Flag

const (
	RunModeFull RunMode = iota
	RunModeSnapshotOnly
	RunModeIncrementalOnly
)

var RunModeIds = map[RunMode][]string{
	RunModeFull:            {"full"},
	RunModeSnapshotOnly:    {"snapshot-only"},
	RunModeIncrementalOnly: {"incremental-only"},
}

// o => dump snapshot => create changefeed =>   load snapshot   => incremental load
//
//	^                 ^                    ^ 				    ^
//	|			      |				       |				    |
//	+-------init------+--snapshot-dumped---+-changefeed-created-+-snapshot-loaded-+
type Stage string

const (
	StageInit              Stage = "init"
	StageSnapshotDumped    Stage = "snapshot-dumped"
	StageChangefeedCreated Stage = "changefeed-created"
	StageSnapshotLoaded    Stage = "snapshot-loaded"
)

func checkStage(storagePath string) (Stage, error) {
	stage := StageInit
	ctx := context.Background()
	storage, err := putil.GetExternalStorageFromURI(ctx, storagePath)
	if err != nil {
		return stage, err
	}
	if exist, err := storage.FileExists(ctx, "snapshot/metadata"); err != nil && exist {
		stage = StageSnapshotDumped
	}
	if exist, err := storage.FileExists(ctx, "increment/metadata"); err != nil && exist {
		stage = StageChangefeedCreated
	}
	if exist, err := storage.FileExists(ctx, "snapshot/loadinfo"); err != nil && exist {
		stage = StageSnapshotLoaded
	}
	return stage, nil
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
		if err != nil {
			return nil, err
		}
		return &credValue, nil
	}
	return nil, errors.New("Not a s3 storage")
}

func Replicate(
	tidbConfig *tidbsql.TiDBConfig,
	tableNames []string,
	storagePath string,
	snapshotConcurrency int,
	cdcHost string,
	cdcPort int,
	cdcFlushInterval time.Duration,
	cdcFileSize int64,
	credValue credentials.Value,
	dwSnapConnectors map[string]coreinterfaces.Connector,
	dwIncreConnectors map[string]coreinterfaces.Connector,
	timezone string,
	mode RunMode,
) error {
	stage, err := checkStage(storagePath)
	if err != nil {
		return errors.Trace(err)
	}

	startTSO, err := tidbsql.GetCurrentTSO(tidbConfig)
	if err != nil {
		return errors.Annotate(err, "Failed to get current TSO")
	}
	snapshotURI, incrementURI, err := genURI(storagePath)
	if err != nil {
		return errors.Trace(err)
	}

	onSnapshotDumpProgress := func(dumpedRows, totalRows int64) {
		log.Info("Snapshot dump progress", zap.Int64("dumpedRows", dumpedRows), zap.Int64("estimatedTotalRows", totalRows))
	}

	switch stage {
	case StageInit:
		if mode != RunModeIncrementalOnly {
			if err := dumpling.RunDump(tidbConfig, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), tableNames, onSnapshotDumpProgress); err != nil {
				return errors.Trace(err)
			}
		}
		fallthrough
	case StageSnapshotDumped:
		if mode != RunModeSnapshotOnly {
			cdcConnector, err := cdc.NewCDCConnector(cdcHost, cdcPort, tableNames, startTSO, incrementURI, cdcFlushInterval, cdcFileSize, &credValue)
			if err != nil {
				return errors.Trace(err)
			}
			if err = cdcConnector.CreateChangefeed(); err != nil {
				return errors.Trace(err)
			}
		}
		fallthrough
	case StageChangefeedCreated:
		if mode != RunModeIncrementalOnly {
			if err = replicate.StartReplicateSnapshot(dwSnapConnectors, tidbConfig, tableNames, snapshotURI, fmt.Sprint(startTSO)); err != nil {
				return errors.Annotate(err, "Failed to replicate snapshot")
			}
		}
		fallthrough
	case StageSnapshotLoaded:
		if mode != RunModeSnapshotOnly {
			if err = replicate.StartReplicateIncrement(dwIncreConnectors, incrementURI, cdcFlushInterval/5, "", timezone); err != nil {
				return errors.Annotate(err, "Failed to replicate incremental")
			}
		}
	}
	return nil
}
