package redshift

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/redshiftsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
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

func NewRedshiftCmd() *cobra.Command {
	var (
		tidbConfigFromCli     tidbsql.TiDBConfig
		redshiftConfigFromCli redshiftsql.RedshiftConfig
		tableFQN              string
		snapshotConcurrency   int
		storagePath           string
		cdcHost               string
		cdcPort               int
		cdcFlushInterval      time.Duration
		cdcFileSize           int64
		timezone              string
		logFile               string
		logLevel              string
		credValue             credentials.Value

		mode RunMode
	)

	run := func() error {
		// 0. check status
		ctx := context.Background()
		storage, err := putil.GetExternalStorageFromURI(ctx, storagePath)
		if err != nil {
			return errors.Trace(err)
		}
		metadataExist, err := storage.FileExists(ctx, "increment/metadata")
		if err != nil {
			return errors.Trace(err)
		}
		loadinfoExist, err := storage.FileExists(ctx, "snapshot/loadinfo")
		if err != nil {
			return errors.Trace(err)
		}

		// 1. get current tso
		startTSO := uint64(0)
		if !loadinfoExist {
			startTSO, err = tidbsql.GetCurrentTSO(&tidbConfigFromCli)
			if err != nil {
				return errors.Annotate(err, "Failed to get current TSO")
			}
		} else {
			log.Info("Snapshot data is all loaded, skip get current TSO")
		}

		var incrementURI *url.URL

		// 2. create changefeed
		if mode == RunModeFull || mode == RunModeIncrementalOnly {
			increStoragePath, err := url.JoinPath(storagePath, "increment")
			if err != nil {
				return errors.Trace(err)
			}
			incrementURI, err = url.Parse(increStoragePath)
			if err != nil {
				return errors.Annotate(err, "Failed to parse workspace path")
			}
			cdcConnector, err := cdc.NewCDCConnector(cdcHost, cdcPort, tableFQN, startTSO, incrementURI, cdcFlushInterval, cdcFileSize, &credValue)
			if err != nil {
				return errors.Trace(err)
			}
			if !loadinfoExist || !metadataExist {
				if err = cdcConnector.CreateChangefeed(); err != nil {
					return errors.Annotate(err, "Failed to create changefeed")
				}
			} else {
				log.Info("Snapshot has been loaded, Changefeed has been created, skip create changefeed")
			}
		}

		var sourceDatabase, sourceTable string
		if tableFQN != "" {
			parts := strings.SplitN(tableFQN, ".", 2)
			if len(parts) != 2 {
				return errors.Errorf("table must be a full-qualified name like mydb.mytable")
			}
			sourceDatabase, sourceTable = parts[0], parts[1]
		}

		// 3. run replicate snapshot
		if (mode == RunModeFull || mode == RunModeSnapshotOnly) && !loadinfoExist {
			snapStoragePath, err := url.JoinPath(storagePath, "snapshot")
			if err != nil {
				return errors.Trace(err)
			}
			snapshotURI, err := url.Parse(snapStoragePath)
			if err != nil {
				return errors.Annotate(err, "Failed to parse workspace path")
			}
			db, err := redshiftConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			connector, err := redshiftsql.NewRedshiftConnector(
				db,
				redshiftConfigFromCli.Schema,
				fmt.Sprintf("snapshot_external_%s", sourceTable),
				redshiftConfigFromCli.Role,
				snapshotURI,
				&credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if err = replicate.StartReplicateSnapshot(connector, &tidbConfigFromCli, sourceDatabase, sourceTable, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), &credValue); err != nil {
				return errors.Annotate(err, "Failed to replicate snapshot")
			}
		} else if !loadinfoExist {
			log.Info("Snapshot has been loaded, skip replicate snapshot")
		}

		// 4. run replicate increment
		if mode == RunModeFull || mode == RunModeIncrementalOnly {
			db, err := redshiftConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			connector, err := redshiftsql.NewRedshiftConnector(
				db,
				redshiftConfigFromCli.Schema,
				fmt.Sprintf("increment_external_%s", sourceTable),
				redshiftConfigFromCli.Role,
				incrementURI,
				&credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if err = replicate.StartReplicateIncrement(connector, incrementURI, cdcFlushInterval/5, "", timezone, &credValue); err != nil {
				return errors.Annotate(err, "Failed to replicate incremental")
			}
		}

		return nil
	}

	cmd := &cobra.Command{
		Use:   "redshift",
		Short: "Replicate snapshot and incremental data from TiDB to Redshift",
		Run: func(_ *cobra.Command, _ []string) {
			// init logger
			err := logutil.InitLogger(&logutil.Config{
				Level: logLevel,
				File:  logFile,
			})
			if err != nil {
				panic(err)
			}

			uri, err := url.Parse(storagePath)
			if err != nil {
				panic(err)
			}
			if uri.Scheme == "s3" {
				// resolve aws credential
				creds := credentials.NewEnvCredentials()
				credValue, err = creds.Get()
				if err != nil {
					panic(err)
				}
			}

			if err = run(); err != nil {
				log.Error("Error running redshift replication", zap.Error(err))
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().Var(enumflag.New(&mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "full", "replication mode: full, snapshot-only, incremental-only")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Host, "tidb.host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.Port, "tidb.port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.User, "tidb.user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Pass, "tidb.pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.SSLCA, "tidb.ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Host, "redshift.host", "redshift-cluster-1.cph4e20x7btf.us-east-1.redshift.amazonaws.com", "redshift host")
	cmd.Flags().IntVar(&redshiftConfigFromCli.Port, "redshift.port", 5439, "redshift port")
	cmd.Flags().StringVar(&redshiftConfigFromCli.User, "redshift.user", "", "redshift user")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Pass, "redshift.pass", "", "redshift password")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Database, "redshift.database", "", "redshift database")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Schema, "redshift.schema", "", "redshift schema")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Role, "redshift.role", "", "iam role for redshift")
	cmd.Flags().StringVarP(&tableFQN, "table", "t", "", "table full qualified name: <database>.<table>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&storagePath, "storage", "s", "", "storage path: s3://<bucket>/<path> or gcs://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", "127.0.0.1", "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", 8300, "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc.flush-interval", 60*time.Second, "")
	cmd.Flags().Int64Var(&cdcFileSize, "cdc.file-size", 64*1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")
	cmd.Flags().StringVar(&logFile, "log.file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log.level", "info", "log level")

	cmd.MarkFlagRequired("storage")
	return cmd
}
