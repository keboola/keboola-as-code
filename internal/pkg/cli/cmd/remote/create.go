package remote

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func CreateCommand(p dependencies.Provider) *cobra.Command {
	createBranchCmd := CreateBranchCommand(p)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: helpmsg.Read(`remote/create/short`),
		Long:  helpmsg.Read(`remote/create/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreateRemote() {
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

func CreateBranchCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: helpmsg.Read(`remote/create/branch/short`),
		Long:  helpmsg.Read(`remote/create/branch/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := p.Dependencies()
			start := time.Now()

			// Options
			options, err := d.Dialogs().AskCreateBranch(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() { eventSender.SendCmdEvent(start, cmdErr, "remote-create-branch") }()
			} else {
				return err
			}

			// Create branch
			branch, err := createBranch.Run(options, d)
			if err != nil {
				return err
			}

			// Run pull, if the command is run in a project directory
			if d.LocalProjectExists() {
				d.Logger().Info()
				d.Logger().Info(`Pulling objects to the local directory.`)

				// Local project
				prj, err := d.LocalProject(false)
				if err != nil {
					return err
				}

				// Project manifest
				projectManifest := prj.ProjectManifest()

				// Add new branch to the allowed branches if needed
				if projectManifest.IsObjectIgnored(branch) {
					allowedBranches := projectManifest.AllowedBranches()
					allowedBranches = append(allowedBranches, model.AllowedBranch(branch.Id.String()))
					projectManifest.SetAllowedBranches(allowedBranches)
				}

				// Load project state - to pull new branch after create
				projectState, err := prj.LoadState(loadState.PullOptions(false))
				if err != nil {
					return err
				}

				// Pull
				pullOptions := pull.Options{DryRun: false, LogUntrackedPaths: false}
				if err := pull.Run(projectState, pullOptions, d); err != nil {
					return utils.PrefixError(`pull failed`, err)
				}
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP("storage-api-host", "H", "", "if command is run outside the project directory")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new branch")
	return cmd
}
