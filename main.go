package main

import (
	sfCmd "github.com/breezewish/tidb-snowflake/cmd/snowflake"
	"github.com/spf13/cobra"
)

var rootCmd *cobra.Command

func init() {
	rootCmd = &cobra.Command{
		Use:   "tidb2dw",
		Short: "A service to replicate from TiDB to Data Warehouse",
	}
	rootCmd.AddCommand(
		sfCmd.NewSnowfalkeCmd(),
	)
}

func main() {
	rootCmd.Execute()
}
