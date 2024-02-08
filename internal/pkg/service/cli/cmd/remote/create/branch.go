package create

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func BranchCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: helpmsg.Read(`remote/create/branch/short`),
		Long:  helpmsg.Read(`remote/create/branch/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateBranch(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-branch")

			// Create branch
			branch, err := createBranch.Run(cmd.Context(), options, d)
			if err != nil {
				return err
			}

			// Run pull, if the command is run in a project directory
			if prj, found, err := d.LocalProject(cmd.Context(), false); found {
				d.Logger().Info(cmd.Context(), "")
				d.Logger().Info(cmd.Context(), `Pulling objects to the local directory.`)

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
				if err := pull.Run(cmd.Context(), projectState, pullOptions, d); err != nil {
					return errors.PrefixError(err, "pull failed")
				}
			}
			return nil
		},
	}

	branchFlags := BranchFlags{}
	_ = cliconfig.GenerateFlags(branchFlags, cmd.Flags())

	return cmd
}
