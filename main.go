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
		SilenceErrors:      true,
		DisableFlagParsing: true,
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
				return cmd.Help()
			}
		},
	}

	rootCmd.Flags().BoolP("version", "v", false, "Print the version of tidb2dw")

	rootCmd.AddCommand(
		cmd.NewSnowflakeCmd(),
		cmd.NewRedshiftCmd(),
	)
}

func main() {
	rootCmd.Execute()
}
