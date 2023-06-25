package snowflake

import "github.com/spf13/cobra"

func NewSnowfalkeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snowflake increment/snapshot",
		Short: "Replicate data from TiDB to Snowflake",
	}

	cmd.AddCommand(
		newIncrementCmd(),
		newSnapshotCmd(),
	)
	return cmd
}
