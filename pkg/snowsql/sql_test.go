package snowsql_test

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/stretchr/testify/require"
)

func TestGenCreateExternalStageSQLDoesNotEmbedS3Secrets(t *testing.T) {
	sql, err := snowsql.GenCreateExternalStageSQL("stage_orders", "s3://bucket/root/snapshot", "tidb2dw_s3_int")

	require.NoError(t, err)
	require.Contains(t, sql, "STORAGE_INTEGRATION")
	require.Contains(t, sql, `"tidb2dw_s3_int"`)
	require.NotContains(t, sql, "AWS_KEY_ID")
	require.NotContains(t, sql, "AWS_SECRET_KEY")
	require.NotContains(t, sql, "AWS_TOKEN")
	require.NotContains(t, sql, "SECRET")
}
