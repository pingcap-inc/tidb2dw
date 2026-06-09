package cmd

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/pin14e2e"
	"github.com/spf13/cobra"
)

func NewPIN14E2ECmd() *cobra.Command {
	var cfg pin14e2e.Config
	var scenarios string

	command := &cobra.Command{
		Use:   "pin14-e2e",
		Short: "Validate and plan PIN-14 Snowflake/S3 fixture e2e runs",
		RunE: func(_ *cobra.Command, _ []string) error {
			if scenarios != "" && scenarios != "all" {
				cfg.Scenarios = splitCSV(scenarios)
			}
			cfg.AWSAccessKeyID = envString("AWS_ACCESS_KEY_ID", "")
			cfg.AWSSecretAccessKey = envString("AWS_SECRET_ACCESS_KEY", "")
			cfg.AWSSessionToken = envString("AWS_SESSION_TOKEN", "")
			result, err := pin14e2e.Runner{Config: cfg}.Run(context.Background())
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(result)
		},
	}

	command.Flags().StringVar((*string)(&cfg.Mode), "mode", envString("PIN14_E2E_MODE", string(pin14e2e.ModeValidate)), "mode: validate, plan, or run")
	command.Flags().StringVar(&cfg.FixtureDir, "fixture-dir", envString("PIN14_FIXTURE_DIR", "testdata/generated/tidb2dw-fixtures"), "fixture output directory")
	command.Flags().StringVar(&cfg.ManifestPath, "manifest", envString("PIN14_MANIFEST_PATH", ""), "fixture manifest path")
	command.Flags().StringVar(&scenarios, "scenarios", envString("PIN14_SCENARIOS", "all"), "comma-separated scenario IDs, or all")
	command.Flags().StringVar(&cfg.StorageURI, "storage-uri", envString("PIN14_S3_STORAGE_URI", ""), "base s3:// URI for staged fixtures")
	command.Flags().StringVar(&cfg.AWSRegion, "aws-region", envString("PIN14_AWS_REGION", envString("AWS_REGION", envString("AWS_DEFAULT_REGION", ""))), "AWS region for S3 client")
	command.Flags().StringVar(&cfg.StagingPrefix, "staging-prefix", envString("PIN14_STAGING_PREFIX", ""), "release-scoped staging prefix")
	command.Flags().StringVar(&cfg.SnowflakeAccountID, "snowflake-account-id", envString("PIN14_SNOWFLAKE_ACCOUNT_ID", ""), "Snowflake account identifier")
	command.Flags().StringVar(&cfg.SnowflakeWarehouse, "snowflake-warehouse", envString("PIN14_SNOWFLAKE_WAREHOUSE", ""), "Snowflake warehouse")
	command.Flags().StringVar(&cfg.SnowflakeDatabase, "snowflake-database", envString("PIN14_SNOWFLAKE_DATABASE", ""), "Snowflake database")
	command.Flags().StringVar(&cfg.SnowflakeSchema, "snowflake-schema", envString("PIN14_SNOWFLAKE_SCHEMA", ""), "Snowflake schema")
	command.Flags().StringVar(&cfg.SnowflakeStorageIntegration, "snowflake-storage-integration", envString("PIN14_SNOWFLAKE_STORAGE_INTEGRATION", ""), "Snowflake storage integration for S3 stages")
	command.Flags().StringVar(&cfg.SnowflakeUser, "snowflake-user", envString("PIN14_SNOWFLAKE_USER", ""), "Snowflake user")
	command.Flags().StringVar(&cfg.SnowflakePassword, "snowflake-password", envString("PIN14_SNOWFLAKE_PASSWORD", ""), "Snowflake password")
	command.Flags().StringVar((*string)(&cfg.CleanupMode), "cleanup-mode", envString("PIN14_CLEANUP_MODE", string(pin14e2e.CleanupAlways)), "cleanup mode: always, on-success, or never")

	return command
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
