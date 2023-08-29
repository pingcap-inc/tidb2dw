package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

func NewSnowflakeCmd() *cobra.Command {
	var (
		tidbConfigFromCli      tidbsql.TiDBConfig
		snowflakeConfigFromCli snowsql.SnowflakeConfig
		tables                 string
		snapshotConcurrency    int
		storagePath            string
		cdcHost                string
		cdcPort                int
		cdcFlushInterval       time.Duration
		cdcFileSize            int64
		timezone               string
		logFile                string
		logLevel               string
		credValue              *credentials.Value

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
			// TODO: support replication to different snowflake databases
			db, err := snowflakeConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			{
				connector, err := snowsql.NewSnowflakeConnector(
					db,
					fmt.Sprintf("snapshot_external_%s", sourceTable),
					snapshotURI,
					credValue,
				)
				if err != nil {
					return errors.Trace(err)
				}
				dwSnapConnectors[tableName] = connector
			}
			{
				connector, err := snowsql.NewSnowflakeConnector(
					db,
					fmt.Sprintf("increment_external_%s", sourceTable),
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
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
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
				log.Error("Error running snowflake replication", zap.Error(err))
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
	cmd.Flags().StringVar(&snowflakeConfigFromCli.AccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Warehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.User, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Pass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Database, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Schema, "snowflake.schema", "", "snowflake schema")
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
