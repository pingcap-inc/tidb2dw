package main

import (
	"fmt"

	sfCmd "github.com/breezewish/tidb-snowflake/cmd/snowflake"
	"github.com/breezewish/tidb-snowflake/version"
	"github.com/spf13/cobra"
)

var rootCmd *cobra.Command

func init() {
	rootCmd = &cobra.Command{
		Use:                "tidb2dw",
		Short:              "A service to replicate from TiDB to Data Warehouse",
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
		sfCmd.NewSnowflakeCmd(),
	)
}

func main() {
	rootCmd.Execute()
}
