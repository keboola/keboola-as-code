package profile

import (
	e "github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
)

type Flag struct {
	TargetName configmap.Value[string] `configKey:"target-name" configShorthand:"T" configUsage:"target name of the profile"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profile`,
		Short: helpmsg.Read(`dbt/generate/profile/short`),
		Long:  helpmsg.Read(`dbt/generate/profile/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

			// Get dependencies
			d := p.BaseScope()

			flag := Flag{}
			err := configmap.Bind(configmap.BindConfig{
				Flags:     cmd.Flags(),
				Args:      args,
				EnvNaming: e.NewNamingConvention("KBC_"),
				Envs:      e.Empty(),
			}, &flag)
			if err != nil {
				return err
			}

			// Ask options
			targetName, err := utils.AskTargetName(d.Dialogs(), flag.TargetName)
			if err != nil {
				return err
			}

			return profile.Run(cmd.Context(), profile.Options{TargetName: targetName}, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
