package cmd

import (
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/taskruntime"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
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
		tables                 []string
		snapshotConcurrency    int
		storagePath            string
		cdcHost                string
		cdcPort                int
		cdcFlushInterval       time.Duration
		cdcFileSize            int
		timezone               string
		logFile                string
		logLevel               string
		awsAccessKey           string
		awsSecretKey           string
		credValue              *credentials.Value
		tenantID               string
		taskID                 string
		stagingPrefix          string
		legacyLayout           bool

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

		tidbEndpoint := fmt.Sprintf("%s:%d", tidbConfigFromCli.Host, tidbConfigFromCli.Port)
		cdcEndpoint := fmt.Sprintf("%s:%d", cdcHost, cdcPort)
		taskCfg := taskruntime.NewSnowflakeTask(
			tenantID,
			taskID,
			storageURI.String(),
			stagingPrefix,
			tables,
			snowflakeConfigFromCli.Database,
			snowflakeConfigFromCli.Schema,
			tidbEndpoint,
			cdcEndpoint,
			snapshotConcurrency,
			len(tables),
		)
		taskCfg.Runtime.UseLegacyLayout = legacyLayout
		plan, err := taskruntime.BuildPlan(taskCfg)
		if err != nil {
			return errors.Trace(err)
		}
		storageURI, err = parsePlanURI(plan.StorageRootURI)
		if err != nil {
			return errors.Trace(err)
		}
		snapshotURI, err := parsePlanURI(plan.SnapshotURI)
		if err != nil {
			return errors.Trace(err)
		}
		incrementURI, err := parsePlanURI(plan.IncrementURI)
		if err != nil {
			return errors.Trace(err)
		}
		checkpointURI, err := checkpointURIForIncrementReplay(plan, legacyLayout)
		if err != nil {
			return errors.Trace(err)
		}
		snowflakeConfigFromCli.Schema = plan.SnowflakeSchema

		snapConnectorMap := make(map[string]coreinterfaces.Connector)
		increConnectorMap := make(map[string]coreinterfaces.Connector)
		for i, tableFQN := range tables {
			tablePlan := plan.Tables[i]
			snapConnector, err := snowsql.NewSnowflakeConnector(
				&snowflakeConfigFromCli,
				tablePlan.SnapshotStage,
				snapshotURI,
			)
			if err != nil {
				return errors.Trace(err)
			}
			snapConnectorMap[tableFQN] = snapConnector
			increConnector, err := snowsql.NewSnowflakeConnector(
				&snowflakeConfigFromCli,
				tablePlan.IncrementStage,
				incrementURI,
			)
			if err != nil {
				return errors.Trace(err)
			}
			increConnectorMap[tableFQN] = increConnector
		}

		defer func() {
			for _, connector := range snapConnectorMap {
				connector.Close()
			}
			for _, connector := range increConnectorMap {
				connector.Close()
			}
		}()

		return Replicate(&tidbConfigFromCli, tables, storageURI, snapshotURI, incrementURI, checkpointURI,
			snapshotConcurrency, cdcHost, cdcPort, cdcFlushInterval, cdcFileSize,
			snapConnectorMap, increConnectorMap, "snowflake", true, mode,
		)
	}

	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
		RunE: func(_ *cobra.Command, _ []string) error {
			if mode != RunModeCloud {
				if err := run(); err != nil {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusFatalError(err)
					log.Error("Fatal error running snowflake replication", zap.Error(err))
					return err
				}
				apiservice.GlobalInstance.APIInfo.SetServiceStatusIdle()
				return nil
			}
			runWithServer(mode == RunModeCloud, fmt.Sprintf("%s:%d", apiListenHost, apiListenPort), func() {
				if err := run(); err != nil {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusFatalError(err)
					log.Error("Fatal error running snowflake replication", zap.Error(err))
				} else {
					apiservice.GlobalInstance.APIInfo.SetServiceStatusIdle()
				}
			})
			return nil
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().Var(enumflag.New(&mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "mode", "replication mode: full, snapshot-only, incremental-only, cloud")
	cmd.Flags().StringVar(&apiListenHost, "api.host", "127.0.0.1", "API service listen host, only available in --mode=cloud")
	cmd.Flags().IntVar(&apiListenPort, "api.port", 8185, "API service listen port, only available in --mode=cloud")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Host, "tidb.host", "h", envString("TIDB2DW_TIDB_HOST", "127.0.0.1"), "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.Port, "tidb.port", "P", envInt("TIDB2DW_TIDB_PORT", 4000), "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.User, "tidb.user", "u", envString("TIDB2DW_TIDB_USER", "root"), "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Pass, "tidb.pass", "p", envString("TIDB2DW_TIDB_PASSWORD", ""), "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.SSLCA, "tidb.ssl-ca", envString("TIDB2DW_TIDB_SSL_CA", ""), "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.AccountId, "snowflake.account-id", envString("TIDB2DW_SNOWFLAKE_ACCOUNT_ID", ""), "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Warehouse, "snowflake.warehouse", envString("TIDB2DW_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"), "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.User, "snowflake.user", envString("TIDB2DW_SNOWFLAKE_USER", ""), "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Pass, "snowflake.pass", envString("TIDB2DW_SNOWFLAKE_PASSWORD", ""), "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Database, "snowflake.database", envString("TIDB2DW_SNOWFLAKE_DATABASE", ""), "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Schema, "snowflake.schema", envString("TIDB2DW_SNOWFLAKE_SCHEMA", ""), "snowflake schema")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.StorageIntegration, "snowflake.storage-integration", envString("TIDB2DW_SNOWFLAKE_STORAGE_INTEGRATION", ""), "Snowflake storage integration used by external stages")
	cmd.Flags().StringVar(&tenantID, "tenant.id", envString("TIDB2DW_TENANT_ID", "default"), "tenant id for task-scoped Snowflake replication state")
	cmd.Flags().StringVar(&taskID, "task.id", envString("TIDB2DW_TASK_ID", "default"), "task id for task-scoped Snowflake replication state")
	cmd.Flags().StringVar(&stagingPrefix, "staging.prefix", envString("TIDB2DW_STAGING_PREFIX", ""), "optional staging prefix before tenant/task scope")
	cmd.Flags().BoolVar(&legacyLayout, "legacy-layout", false, "preserve legacy snapshot/increment storage layout instead of tenant/task scoped layout")
	cmd.Flags().StringArrayVarP(&tables, "table", "t", []string{}, "tables full qualified name, e.g. -t <db1>.<table1> -t <db2>.<table2>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&storagePath, "storage", "s", "", "storage path: s3://<bucket>/<path> or gcs://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", envString("TIDB2DW_CDC_HOST", "127.0.0.1"), "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", envInt("TIDB2DW_CDC_PORT", 8300), "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc.flush-interval", 60*time.Second, "")
	cmd.Flags().IntVar(&cdcFileSize, "cdc.file-size", 64*1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")
	cmd.Flags().StringVar(&logFile, "log.file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log.level", "info", "log level")
	cmd.Flags().StringVar(&awsAccessKey, "aws.access-key", "", "aws access key")
	cmd.Flags().StringVar(&awsSecretKey, "aws.secret-key", "", "aws secret key")

	cmd.MarkFlagRequired("storage")

	return cmd
}

func checkpointURIForIncrementReplay(plan *taskruntime.RuntimePlan, legacyLayout bool) (*url.URL, error) {
	if legacyLayout {
		return nil, nil
	}
	return parsePlanURI(plan.CheckpointURI)
}
