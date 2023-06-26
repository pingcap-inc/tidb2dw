package snowflake

import "github.com/spf13/cobra"

func NewSnowfalkeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snowflake <command> [args...]",
		Short: "Replicate data from TiDB to Snowflake",
	}

	cmd.AddCommand(
		newIncrementCmd(),
		newSnapshotCmd(),
	)
	return cmd
}
