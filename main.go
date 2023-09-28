package main

import (
	"fmt"

	"github.com/pingcap-inc/tidb2dw/cmd"
	"github.com/pingcap-inc/tidb2dw/version"
	"github.com/spf13/cobra"
)

var rootCmd *cobra.Command

func init() {
	rootCmd = &cobra.Command{
		Use:                "tidb2dw",
		Short:              "A service to replicate data changes from TiDB to Data Warehouse in real-time",
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			switch args[0] {
			case "--help", "-h":
				return cmd.Help()
			case "--version", "-v":
				fmt.Println(version.NewTiDB2DWVersion().String())
				return nil
			default:
				return fmt.Errorf("unknown flag: %s\nRun `tidb2dw --help` for usage.", args[0])
			}
		},
	}

	rootCmd.Flags().BoolP("version", "v", false, "Print the version of tidb2dw")

	rootCmd.AddCommand(
		// replications
		cmd.NewSnowflakeCmd(),
		cmd.NewRedshiftCmd(),
		cmd.NewBigQueryCmd(),
		cmd.NewDatabricksCmd(),

		// exporters
		cmd.NewS3Cmd(),
		cmd.NewGCSCmd(),
	)
}

func main() {
	rootCmd.Execute()
}
