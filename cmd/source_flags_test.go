package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultSourceOptions(t *testing.T) {
	opts := defaultSourceOptions()

	require.Equal(t, SourceFormatCSV, opts.format)
	require.Equal(t, 5*time.Second, opts.icebergPollInterval)
}
