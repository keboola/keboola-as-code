package config

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	Branch      configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	ComponentID configmap.Value[string] `configKey:"component-id" configShorthand:"c" configUsage:"component ID"`
	Name        configmap.Value[string] `configKey:"name" configShorthand:"n" configUsage:"name of the new config"`
}

// nolint: dupl
func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: helpmsg.Read(`local/create/config/short`),
		Long:  helpmsg.Read(`local/create/config/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// flags
			f := Flags{}
			if err = configmap.Bind(utils.GetBindConfig(cmd.Flags(), args), &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options, err := AskCreateConfig(projectState, d.Dialogs(), d, f)
			if err != nil {
				return err
			}

			// Create config
			return createConfig.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
