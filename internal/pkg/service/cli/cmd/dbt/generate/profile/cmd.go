package profile

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
)

type Flags struct {
	TargetName configmap.Value[string] `configKey:"target-name" configShorthand:"T" configUsage:"target name of the profile"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profile`,
		Short: helpmsg.Read(`dbt/generate/profile/short`),
		Long:  helpmsg.Read(`dbt/generate/profile/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

			// Get dependencies
			d := p.BaseScope()

			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Ask options
			targetName, err := p.BaseScope().Dialogs().AskTargetName(f.TargetName)
			if err != nil {
				return err
			}

			return profile.Run(cmd.Context(), profile.Options{TargetName: targetName}, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
