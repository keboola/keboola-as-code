package generate

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
)

func ProfileCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profile`,
		Short: helpmsg.Read(`dbt/generate/profile/short`),
		Long:  helpmsg.Read(`dbt/generate/profile/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			// Options
			targetName, err := d.Dialogs().AskTargetName(d)
			if err != nil {
				return err
			}

			return profile.Run(d.CommandCtx(), targetName, d)
		},
	}

	cmd.Flags().StringP("target-name", "T", "", "target name of the profile")

	return cmd
}
