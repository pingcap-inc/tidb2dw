package snowflake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

func genSinkURI(s3StoragePath string, flushInterval time.Duration, fileSize int64) (*url.URL, error) {
	cdcPath, err := url.Parse(path.Join(s3StoragePath, "increment"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	values := cdcPath.Query()
	values.Add("protocol", "csv")
	values.Add("flush-interval", flushInterval.String())
	values.Add("file-size", fmt.Sprint(fileSize))
	cdcPath.RawQuery = values.Encode()
	return cdcPath, nil
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
	}
	{
		data["start_ts"] = startTSO
	}
	bytesData, _ := json.Marshal(data)
	req, _ := http.NewRequest("POST", path.Join(cdcServer, "api/v2/changefeeds"), bytes.NewReader(bytesData))
	resp, err := client.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("create changefeed failed, status code: %d", resp.StatusCode)
	}
	return nil
}

func newFullCmd() *cobra.Command {
	var (
		tidbConfigFromCli   TiDBConfig
		tableFQN            string
		snapshotConcurrency int
		s3StoragePath       string
		cdcServer           string
		cdcFlushInterval    time.Duration
		cdcFileSize         int64
		timezone            string
		startTSO            uint64
	)

	cmd := &cobra.Command{
		Use:   "full",
		Short: "Replicate both snapshot and incremental data from TiDB to Snowflake",
		Run: func(_ *cobra.Command, _ []string) {
			session, err := NewReplicateSession(&snowflakeConfigFromCli, &tidbConfigFromCli, tableFQN, snapshotConcurrency, path.Join(s3StoragePath, "snapshot"))
			if err != nil {
				panic(err)
			}
			defer session.Close()

			// create changefeed
			sinkURI, err := genSinkURI(s3StoragePath, cdcFlushInterval, cdcFileSize)
			if err != nil {
				panic(err)
			}
			err = createChangefeed(cdcServer, sinkURI, tableFQN, startTSO)
			if err != nil {
				panic(err)
			}

			// run replicate snapshot
			err = session.Run()
			if err != nil {
				panic(err)
			}

			// run replicate increment
			err = startReplicateIncrement(sinkURI, cdcFlushInterval, "", timezone)
			if err != nil {
				panic(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBHost, "host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.TiDBPort, "port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBUser, "user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBPass, "pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.TiDBSSLCA, "ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeAccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeWarehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeUser, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakePass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeDatabase, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeSchema, "snowflake.schema", "", "snowflake schema")
	cmd.Flags().StringVarP(&tableFQN, "table", "t", "", "table full qualified name: <database>.<table>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&s3StoragePath, "storage", "s", "", "S3 storage path: s3://<bucket>/<path>")
	cmd.Flags().Uint64Var(&startTSO, "start-ts", 0, "")
	cmd.Flags().StringVar(&cdcServer, "cdc-server", "", "TiCDC server address")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc-flush-interval", 60*time.Second, "")
	cmd.Flags().Int64Var(&cdcFileSize, "cdc-file-size", 1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")

	cmd.MarkFlagRequired("storage")
	cmd.MarkFlagRequired("table")
	return cmd
}
