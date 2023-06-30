package snowflake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/google/uuid"
	"github.com/pingcap-inc/tidb2dw/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func genSinkURI(s3StoragePath string, flushInterval time.Duration, fileSize int64) (*url.URL, error) {
	sinkUri, err := url.Parse(s3StoragePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	values := sinkUri.Query()
	values.Add("flush-interval", flushInterval.String())
	values.Add("file-size", fmt.Sprint(fileSize))
	values.Add("protocol", "csv")
	if sinkUri.Scheme == "s3" {
		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		if err != nil {
			log.Error("Failed to resolve AWS credential", zap.Error(err))
		}
		values.Add("access-key", credValue.AccessKeyID)
		values.Add("secret-access-key", credValue.SecretAccessKey)
		values.Add("session-token", credValue.SessionToken)
	}
	sinkUri.RawQuery = values.Encode()
	return sinkUri, nil
}

func createChangefeed(cdcServer string, sinkURI *url.URL, tableFQN string, startTSO uint64) error {
	client := &http.Client{}
	data := make(map[string]interface{})
	{
		data["sink_uri"] = sinkURI.String()
	}
	{
		replicateConfig := make(map[string]interface{})
		filterConfig := make(map[string]interface{})
		filterConfig["rules"] = []string{tableFQN}
		replicateConfig["filter"] = filterConfig
		csvConfig := make(map[string]interface{})
		csvConfig["include_commit_ts"] = true
		sinkConfig := make(map[string]interface{})
		sinkConfig["csv"] = csvConfig
		replicateConfig["sink"] = sinkConfig
		data["replica_config"] = replicateConfig
	}
	if startTSO != 0 {
		data["start_ts"] = startTSO
	}
	bytesData, _ := json.Marshal(data)
	url, err := url.JoinPath(cdcServer, "api/v2/changefeeds")
	if err != nil {
		return errors.Annotate(err, "join url failed")
	}
	req, _ := http.NewRequest("POST", url, bytes.NewReader(bytesData))
	resp, err := client.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("create changefeed failed, status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	respData := make(map[string]interface{})
	if err = json.Unmarshal([]byte(body), &respData); err != nil {
		return errors.Trace(err)
	}
	changefeedID, _ := respData["id"].(string)
	log.Info("create changefeed success", zap.String("changefeed-id", changefeedID), zap.Any("resp", respData))

	return nil
}

func newFullCmd() *cobra.Command {
	var (
		tidbConfigFromCli   tidbsql.TiDBConfig
		tableFQN            string
		snapshotConcurrency int
		s3StoragePath       string
		cdcHost             string
		cdcPort             int
		cdcFlushInterval    time.Duration
		cdcFileSize         int64
		timezone            string
		logFile             string
		logLevel            string
	)

	cmd := &cobra.Command{
		Use:   "full",
		Short: "Replicate both snapshot and incremental data from TiDB to Snowflake",
		Run: func(_ *cobra.Command, _ []string) {
			err := logutil.InitLogger(&logutil.Config{
				Level: logLevel,
				File:  logFile,
			})
			if err != nil {
				panic(err)
			}

			db, err := tidbsql.OpenTiDB(&tidbConfigFromCli)
			if err != nil {
				panic(err)
			}
			defer db.Close()

			// get current tso
			startTSO, err := tidbsql.GetCurrentTSO(db)
			if err != nil {
				panic(err)
			}

			// enlarge gc duration
			if err = tidbsql.EnlargeGCDuration(db); err != nil {
				panic(err)
			}
			// make sure reset gc duration
			defer tidbsql.ResetGCDuration(db)

			// generate uuid, and append to s3 path
			uid := uuid.New().String()
			s3StoragePath, err := url.JoinPath(s3StoragePath, uid)
			if err != nil {
				panic(err)
			}

			// create changefeed
			increS3StoragePath, err := url.JoinPath(s3StoragePath, "increment")
			if err != nil {
				panic(err)
			}
			sinkURI, err := genSinkURI(increS3StoragePath, cdcFlushInterval, cdcFileSize)
			if err != nil {
				panic(err)
			}
			if err = createChangefeed(fmt.Sprintf("http://%s:%d", cdcHost, cdcPort), sinkURI, tableFQN, startTSO); err != nil {
				panic(err)
			}

			// run replicate snapshot
			snapS3StoragePath, err := url.JoinPath(s3StoragePath, "snapshot")
			if err != nil {
				panic(err)
			}
			if err = startReplicateSnapshot(&snowflakeConfigFromCli, &tidbConfigFromCli, tableFQN, snapshotConcurrency, snapS3StoragePath, fmt.Sprint(startTSO)); err != nil {
				panic(err)
			}

			// reset gc duration
			if err = tidbsql.ResetGCDuration(db); err != nil {
				panic(err)
			}

			// run replicate increment
			if err = startReplicateIncrement(sinkURI, cdcFlushInterval/5, "", timezone); err != nil {
				panic(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBHost, "tidb.host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.TiDBPort, "tidb.port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBUser, "tidb.user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBPass, "tidb.pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.TiDBSSLCA, "tidb.ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeAccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeWarehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeUser, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakePass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeDatabase, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeSchema, "snowflake.schema", "", "snowflake schema")
	cmd.Flags().StringVarP(&tableFQN, "table", "t", "", "table full qualified name: <database>.<table>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&s3StoragePath, "storage", "s", "", "S3 storage path: s3://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", "127.0.0.1", "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", 8300, "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc-flush-interval", 60*time.Second, "")
	cmd.Flags().Int64Var(&cdcFileSize, "cdc-file-size", 64*1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")
	cmd.Flags().StringVar(&logFile, "log-file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level")

	cmd.MarkFlagRequired("storage")
	cmd.MarkFlagRequired("table")
	return cmd
}
