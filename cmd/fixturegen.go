package cmd

import (
	"fmt"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/testfixtures"
	"github.com/spf13/cobra"
)

func NewFixtureGenCmd() *cobra.Command {
	var (
		outputDir string
		seed      int64
		profile   string
		scenarios string
		noClean   bool
	)

	command := &cobra.Command{
		Use:   "fixturegen",
		Short: "Generate deterministic TiDB snapshot and CDC fixtures for DDL validation",
		RunE: func(_ *cobra.Command, _ []string) error {
			scenarioIDs := []string{}
			if scenarios != "" && scenarios != "all" {
				for _, scenario := range strings.Split(scenarios, ",") {
					scenario = strings.TrimSpace(scenario)
					if scenario != "" {
						scenarioIDs = append(scenarioIDs, scenario)
					}
				}
			}
			result, err := testfixtures.Generate(testfixtures.Options{
				OutputDir:   outputDir,
				Seed:        seed,
				Profile:     profile,
				ScenarioIDs: scenarioIDs,
				CleanOutput: !noClean,
			})
			if err != nil {
				return err
			}
			fmt.Printf("generated %d validation manifests under %s\n", len(result.Manifests), result.OutputDir)
			return nil
		},
	}

	command.Flags().StringVar(&outputDir, "output-dir", "testdata/generated/tidb2dw-fixtures", "fixture output directory")
	command.Flags().Int64Var(&seed, "seed", 1, "deterministic fixture seed")
	command.Flags().StringVar(&profile, "profile", testfixtures.ProfileSmoke, "fixture profile: smoke or e2e")
	command.Flags().StringVar(&scenarios, "scenarios", "all", "comma-separated PIN-10 scenario IDs, or all")
	command.Flags().BoolVar(&noClean, "no-clean", false, "overwrite fixture files without removing generated output folders first")

	return command
}
