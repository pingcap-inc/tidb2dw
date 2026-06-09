package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvStringUsesFallbackForMissingOrEmptyValue(t *testing.T) {
	t.Setenv("TIDB2DW_TEST_STRING", "")

	require.Equal(t, "fallback", envString("TIDB2DW_TEST_STRING", "fallback"))
	require.Equal(t, "fallback", envString("TIDB2DW_TEST_MISSING_STRING", "fallback"))
}

func TestEnvStringUsesPresentValue(t *testing.T) {
	t.Setenv("TIDB2DW_TEST_STRING", "present")

	require.Equal(t, "present", envString("TIDB2DW_TEST_STRING", "fallback"))
}

func TestEnvIntUsesFallbackForMissingEmptyOrInvalidValue(t *testing.T) {
	t.Setenv("TIDB2DW_TEST_INT_EMPTY", "")
	t.Setenv("TIDB2DW_TEST_INT_INVALID", "not-a-number")

	require.Equal(t, 4000, envInt("TIDB2DW_TEST_INT_EMPTY", 4000))
	require.Equal(t, 4000, envInt("TIDB2DW_TEST_INT_MISSING", 4000))
	require.Equal(t, 4000, envInt("TIDB2DW_TEST_INT_INVALID", 4000))
}

func TestEnvIntUsesPresentValue(t *testing.T) {
	t.Setenv("TIDB2DW_TEST_INT", "8300")

	require.Equal(t, 8300, envInt("TIDB2DW_TEST_INT", 4000))
}
