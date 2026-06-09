package pin14e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateStageSQLPrefersStorageIntegration(t *testing.T) {
	sql := createStageSQL("PIN14_STAGE", "s3://bucket/root", stageCredentialMode{
		storageIntegration: "PIN14_S3_INT",
		awsAccessKeyID:     "aws-key",
		awsSecretAccessKey: "aws-secret",
	})

	require.Contains(t, sql, `STORAGE_INTEGRATION = "PIN14_S3_INT"`)
	require.NotContains(t, sql, "CREDENTIALS")
	require.NotContains(t, sql, "aws-secret")
}

func TestCreateStageSQLUsesAccessKeyCredentialsWithoutStorageIntegration(t *testing.T) {
	sql := createStageSQL("PIN14_STAGE", "s3://bucket/root", stageCredentialMode{
		awsAccessKeyID:     "aws-key",
		awsSecretAccessKey: "aws-secret",
		awsSessionToken:    "aws-token",
	})

	require.Contains(t, sql, "CREDENTIALS = (")
	require.Contains(t, sql, "AWS_KEY_ID='aws-key'")
	require.Contains(t, sql, "AWS_SECRET_KEY='aws-secret'")
	require.Contains(t, sql, "AWS_TOKEN='aws-token'")
	require.NotContains(t, sql, "STORAGE_INTEGRATION")
}

func TestSanitizeForReportRedactsStageCredentialsAndDerivedURLs(t *testing.T) {
	msg := "failed for s3://bucket/root/path with AWS_KEY_ID='aws-key' AWS_SECRET_KEY='aws-secret' AWS_TOKEN='aws-token' host=acct.snowflakecomputing.com"

	sanitized := sanitizeForReport(msg)

	require.NotContains(t, sanitized, "bucket")
	require.NotContains(t, sanitized, "aws-key")
	require.NotContains(t, sanitized, "aws-secret")
	require.NotContains(t, sanitized, "aws-token")
	require.NotContains(t, sanitized, "acct.snowflakecomputing.com")
	require.Contains(t, sanitized, "s3://<redacted>")
}
