package cmd

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestDefaultSourceOptions(t *testing.T) {
	opts := defaultSourceOptions()

	require.Equal(t, SourceFormatCSV, opts.format)
	require.Equal(t, 5*time.Second, opts.icebergPollInterval)
}

func TestValidateSourceOptionsRejectsIceberg(t *testing.T) {
	opts := defaultSourceOptions()
	opts.format = SourceFormatIceberg

	err := validateSourceOptions(opts)
	require.ErrorContains(t, err, "source-format=iceberg is not implemented")
}

func TestWarehouseCommandsRegisterSourceFlags(t *testing.T) {
	commands := []*cobra.Command{
		NewSnowflakeCmd(),
		NewRedshiftCmd(),
		NewBigQueryCmd(),
		NewDatabricksCmd(),
	}

	for _, cmd := range commands {
		require.NotNil(t, cmd.Flags().Lookup("source-format"), cmd.Name())
		require.NotNil(t, cmd.Flags().Lookup("iceberg.source-uri"), cmd.Name())
		require.NotNil(t, cmd.Flags().Lookup("iceberg.poll-interval"), cmd.Name())
	}
}

func TestWarehouseCommandsRejectIcebergSourceFormat(t *testing.T) {
	commands := []*cobra.Command{
		NewSnowflakeCmd(),
		NewRedshiftCmd(),
		NewBigQueryCmd(),
		NewDatabricksCmd(),
	}

	for _, cmd := range commands {
		err := cmd.Flags().Set("source-format", string(SourceFormatIceberg))
		require.NoError(t, err, cmd.Name())
		require.NotNil(t, cmd.PreRunE, cmd.Name())
		err = cmd.PreRunE(cmd, nil)
		require.ErrorContains(t, err, "source-format=iceberg is not implemented", cmd.Name())
	}
}
