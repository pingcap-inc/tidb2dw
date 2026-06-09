package apiservice

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIInfoRedactsCredentialBearingErrors(t *testing.T) {
	info := NewAPIInfo()
	err := errors.New("failed s3://bucket/root?access-key=AKIA&secret-access-key=SECRET&session-token=TOKEN snowflake.pass=password token=abc")

	info.SetServiceStatusFatalError(err)
	info.SetTableFatalError("sales.orders", err)

	require.NotContains(t, info.r.ErrorMessage, "AKIA")
	require.NotContains(t, info.r.ErrorMessage, "SECRET")
	require.NotContains(t, info.r.ErrorMessage, "TOKEN")
	require.NotContains(t, info.r.ErrorMessage, "password")
	require.NotContains(t, info.r.TablesInfo["sales.orders"].ErrorMessage, "SECRET")
	require.Contains(t, info.r.ErrorMessage, "<redacted>")
}
