package snowflake

import "github.com/spf13/cobra"

func NewSnowflakeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate data from TiDB to Snowflake",
	}

	cmd.AddCommand(
		newFullCmd(),
		newSnapshotCmd(),
		newIncrementCmd(),
	)
	return cmd
}
