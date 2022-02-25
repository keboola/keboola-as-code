package remote

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

func CreateCommand(depsProvider dependencies.Provider) *cobra.Command {
	createBranchCmd := CreateBranchCommand(depsProvider)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/create/short`),
		Long:  helpmsg.Read(`remote/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Project is required
			if _, err := d.LocalProject(false); err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreateLocal() {
			case `branch`:
				return createBranchCmd.RunE(createBranchCmd, nil)
			default:
				// Non-interactive terminal -> print sub-commands.
				return cmd.Help()
			}
		},
	}

	cmd.AddCommand(createBranchCmd)
	return cmd
}

func CreateBranchCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: helpmsg.Read(`remote/create/branch/short`),
		Long:  helpmsg.Read(`remote/create/branch/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()

			// Project is required
			if _, err := d.LocalProject(false); err != nil {
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
