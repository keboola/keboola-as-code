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
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

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
			start := time.Now()
			d, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateBranch(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer func() { d.EventSender().SendCmdEvent(d.CommandCtx(), start, cmdErr, "remote-create-branch") }()

			// Create branch
			branch, err := createBranch.Run(d.CommandCtx(), options, d)
			if err != nil {
				return err
			}

			// Run pull, if the command is run in a project directory
			if prj, found, err := d.LocalProject(false); found {
				d.Logger().Info()
				d.Logger().Info(`Pulling objects to the local directory.`)

				// Local project
				if err != nil {
					return err
				}

				// Project manifest
				projectManifest := prj.ProjectManifest()

				// Add new branch to the allowed branches if needed
				if !projectManifest.AllowedBranches().IsBranchAllowed(model.NewBranch(branch)) {
					allowedBranches := projectManifest.AllowedBranches()
					allowedBranches = append(allowedBranches, model.AllowedBranch(branch.ID.String()))
					projectManifest.SetAllowedBranches(allowedBranches)
				}

				// Load project state - to pull new branch after create
				projectState, err := prj.LoadState(loadState.PullOptions(false), d)
				if err != nil {
					return err
				}

				// Pull
				pullOptions := pull.Options{DryRun: false, LogUntrackedPaths: false}
				if err := pull.Run(d.CommandCtx(), projectState, pullOptions, d); err != nil {
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
