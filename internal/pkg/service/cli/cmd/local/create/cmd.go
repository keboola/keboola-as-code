package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type ConfigFlags struct {
	Branch      string `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	ComponentID string `configKey:"component-id" configShorthand:"c" configUsage:"component ID"`
	Name        string `configKey:"name" configShorthand:"n" configUsage:"name of the new config"`
}

type RowFlags struct {
	Branch string `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Config string `configKey:"config" configShorthand:"c" configUsage:"config name or ID"`
	Name   string `configKey:"name" configShorthand:"n" configUsage:"name of the new config row"`
}

func CreateCommand(p dependencies.Provider) *cobra.Command {
	createConfigCmd := ConfigCommand(p)
	createRowCmd := RowCommand(p)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`local/create/short`),
		Long:  helpmsg.Read(`local/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// CreateCommand must be used in project directory
			_, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreateLocal() {
			case `config`:
				return createConfigCmd.RunE(createConfigCmd, nil)
			case `config row`:
				return createRowCmd.RunE(createRowCmd, nil)
			default:
				// Non-interactive terminal -> print sub-commands.
				return cmd.Help()
			}
		},
	}

	cmd.AddCommand(createConfigCmd, createRowCmd)
	return cmd
}

// nolint: dupl
func ConfigCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: helpmsg.Read(`local/create/config/short`),
		Long:  helpmsg.Read(`local/create/config/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// CreateCommand must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateConfig(projectState, d)
			if err != nil {
				return err
			}

			// Create config
			return createConfig.Run(cmd.Context(), projectState, options, d)
		},
	}

	cliconfig.MustGenerateFlags(ConfigFlags{}, cmd.Flags())

	return cmd
}

// nolint: dupl
func RowCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "row",
		Short: helpmsg.Read(`local/create/row/short`),
		Long:  helpmsg.Read(`local/create/row/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// CreateCommand must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateRow(projectState, d)
			if err != nil {
				return err
			}

			// Create row
			return createRow.Run(cmd.Context(), projectState, options, d)
		},
	}

	cliconfig.MustGenerateFlags(RowFlags{}, cmd.Flags())

	return cmd
}
