package generate

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
)

type ProfileFlag struct {
	TargetName string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
}

func ProfileCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profile`,
		Short: helpmsg.Read(`dbt/generate/profile/short`),
		Long:  helpmsg.Read(`dbt/generate/profile/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

			// Ask options
			targetName, err := p.BaseScope().Dialogs().AskTargetName()
			if err != nil {
				return err
			}

			// Get dependencies
			d := p.BaseScope()

			return profile.Run(cmd.Context(), profile.Options{TargetName: targetName}, d)
		},
	}

	cliconfig.MustGenerateFlags(ProfileFlag{}, cmd.Flags())

	return cmd
}
