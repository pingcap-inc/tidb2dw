package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
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
		tables                string
		snapshotConcurrency   int
		storagePath           string
		cdcHost               string
		cdcPort               int
		cdcFlushInterval      time.Duration
		cdcFileSize           int64
		timezone              string
		logFile               string
		logLevel              string
		credValue             *credentials.Value

		mode RunMode
	)

	run := func() error {
		snapshotURI, incrementURI, err := genURI(storagePath)
		if err != nil {
			return errors.Trace(err)
		}

		tableNames := strings.Split(tables, ",")
		dwSnapConnectors := make(map[string]coreinterfaces.Connector, len(tableNames))
		dwIncreConnectors := make(map[string]coreinterfaces.Connector, len(tableNames))
		for _, tableName := range tableNames {
			_, sourceTable := utils.SplitTableFQN(tableName)
			db, err := redshiftConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			{
				connector, err := redshiftsql.NewRedshiftConnector(
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
				dwSnapConnectors[tableName] = connector
			}
			{
				connector, err := redshiftsql.NewRedshiftConnector(
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
				dwIncreConnectors[tableName] = connector
			}
		}
		return Replicate(&tidbConfigFromCli, tableNames, storagePath, snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize, *credValue, dwSnapConnectors, dwIncreConnectors, timezone, mode)
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

			credValue, err = resolveAWSCredential(storagePath)
			if err != nil {
				panic(err)
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
	cmd.Flags().StringVarP(&tables, "tables", "t", "", "tables full qualified name: <db_1>.<t_a>, <db_2>.<t_b>, ...")
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
