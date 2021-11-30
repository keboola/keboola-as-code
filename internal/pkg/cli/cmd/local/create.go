package local

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/remote/create/branch"
)

func CreateCommand(depsProvider dependencies.Provider) *cobra.Command {
	createBranchCmd := CreateBranchCommand(depsProvider)
	createConfigCmd := CreateConfigCommand(depsProvider)
	createRowCmd := CreateRowCommand(depsProvider)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`local/create/short`),
		Long:  helpmsg.Read(`local/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreate() {
			case `branch`:
				return createBranchCmd.RunE(createBranchCmd, nil)
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

	cmd.AddCommand(createBranchCmd, createConfigCmd, createRowCmd)
	return cmd
}

func CreateBranchCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: helpmsg.Read(`local/create/branch/short`),
		Long:  helpmsg.Read(`local/create/branch/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateBranch(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "create-branch")
				}()
			} else {
				return err
			}

			// Create branch
			return createBranch.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`name`, "n", ``, "name of the new branch")
	return cmd
}

// nolint: dupl
func CreateConfigCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: helpmsg.Read(`local/create/config/short`),
		Long:  helpmsg.Read(`local/create/config/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateConfig(d, createConfig.LoadStateOptions())
			if err != nil {
				return err
			}

			// Create config
			return createConfig.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`component-id`, "c", ``, "component ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config")
	return cmd
}

// nolint: dupl
func CreateRowCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "row",
		Short: helpmsg.Read(`local/create/row/short`),
		Long:  helpmsg.Read(`local/create/row/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateRow(d, createRow.LoadStateOptions())
			if err != nil {
				return err
			}

			// Create row
			return createRow.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`config`, "c", ``, "config name or ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config row")
	return cmd
}
