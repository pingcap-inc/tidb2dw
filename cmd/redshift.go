package cmd

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/redshiftsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

func NewRedshiftCmd() *cobra.Command {
	var (
		tidbConfigFromCli     tidbsql.TiDBConfig
		redshiftConfigFromCli redshiftsql.RedshiftConfig
		tables                []string
		snapshotConcurrency   int
		storagePath           string
		cdcHost               string
		cdcPort               int
		cdcFlushInterval      time.Duration
		cdcFileSize           int64
		timezone              string
		logFile               string
		logLevel              string
		awsAccessKey          string
		awsSecretKey          string
		credValue             *credentials.Value

		mode          RunMode
		apiListenHost string
		apiListenPort int
	)

	run := func() error {
		err := logutil.InitLogger(&logutil.Config{
			Level: logLevel,
			File:  logFile,
		})
		if err != nil {
			return errors.Trace(err)
		}

		if awsAccessKey != "" && awsSecretKey != "" {
			credValue = &credentials.Value{
				AccessKeyID:     awsAccessKey,
				SecretAccessKey: awsSecretKey,
			}
		} else {
			credValue, err = resolveAWSCredential(storagePath)
			if err != nil {
				return errors.Trace(err)
			}
		}

		storageURI, err := getS3URIWithCredentials(storagePath, credValue)
		if err != nil {
			return errors.Trace(err)
		}

		snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(storageURI)
		if err != nil {
			return errors.Trace(err)
		}

		snapConnectorMap := make(map[string]coreinterfaces.Connector)
		increConnectorMap := make(map[string]coreinterfaces.Connector)
		for _, tableFQN := range tables {
			_, sourceTable := utils.SplitTableFQN(tableFQN)
			db, err := redshiftConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			snapConnector, err := redshiftsql.NewRedshiftConnector(
				db,
				redshiftConfigFromCli.Schema,
				fmt.Sprintf("snapshot_external_%s", sourceTable),
				redshiftConfigFromCli.Role,
				snapshotURI,
				credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			snapConnectorMap[tableFQN] = snapConnector
			db, err = redshiftConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			increConnector, err := redshiftsql.NewRedshiftConnector(
				db,
				redshiftConfigFromCli.Schema,
				fmt.Sprintf("increment_external_%s", sourceTable),
				redshiftConfigFromCli.Role,
				incrementURI,
				credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			increConnectorMap[tableFQN] = increConnector
		}

		return Replicate(&tidbConfigFromCli, tables, storageURI, snapshotURI, incrementURI,
			snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize,
			snapConnectorMap, increConnectorMap, mode)
	}

	cmd := &cobra.Command{
		Use:   "redshift",
		Short: "Replicate snapshot and incremental data from TiDB to Redshift",
		Run: func(_ *cobra.Command, _ []string) {
			runWithServer(mode == RunModeCloud, fmt.Sprintf("%s:%d", apiListenHost, apiListenPort), func() {
				if err := run(); err != nil {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusFatalError(err)
					log.Error("Fatal error running redshift replication", zap.Error(err))
				} else {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusIdle()
				}
			})
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().Var(enumflag.New(&mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "mode", "replication mode: full, snapshot-only, incremental-only, cloud")
	cmd.Flags().StringVar(&apiListenHost, "api.host", "0.0.0.0", "API service listen host, only available in --mode=cloud")
	cmd.Flags().IntVar(&apiListenPort, "api.port", 8185, "API service listen port, only available in --mode=cloud")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Host, "tidb.host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.Port, "tidb.port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.User, "tidb.user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Pass, "tidb.pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.SSLCA, "tidb.ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Host, "redshift.host", "", "redshift host")
	cmd.Flags().IntVar(&redshiftConfigFromCli.Port, "redshift.port", 5439, "redshift port")
	cmd.Flags().StringVar(&redshiftConfigFromCli.User, "redshift.user", "", "redshift user")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Pass, "redshift.pass", "", "redshift password")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Database, "redshift.database", "", "redshift database")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Schema, "redshift.schema", "", "redshift schema")
	cmd.Flags().StringVar(&redshiftConfigFromCli.Role, "redshift.role", "", "iam role for redshift")
	cmd.Flags().StringArrayVarP(&tables, "table", "t", []string{}, "tables full qualified name, e.g. -t <db1>.<table1> -t <db2>.<table2>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&storagePath, "storage", "s", "", "storage path: s3://<bucket>/<path> or gcs://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", "127.0.0.1", "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", 8300, "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc.flush-interval", 60*time.Second, "")
	cmd.Flags().Int64Var(&cdcFileSize, "cdc.file-size", 64*1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")
	cmd.Flags().StringVar(&logFile, "log.file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log.level", "info", "log level")
	cmd.Flags().StringVar(&awsAccessKey, "aws.access-key", "", "aws access key")
	cmd.Flags().StringVar(&awsSecretKey, "aws.secret-key", "", "aws secret key")

	cmd.MarkFlagRequired("storage")
	cmd.MarkFlagRequired("redshift.host")
	return cmd
}
