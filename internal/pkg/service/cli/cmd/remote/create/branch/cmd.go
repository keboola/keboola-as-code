package branch

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"if command is run outside the project directory"`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Name            configmap.Value[string] `configKey:"name" configShorthand:"n" configUsage:"name of the new branch"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: helpmsg.Read(`remote/create/branch/short`),
		Long:  helpmsg.Read(`remote/create/branch/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Options
			options, err := AskCreateBranch(d.Dialogs(), f.Name)
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

			// Get local project
			prj, found, err := d.LocalProject(cmd.Context(), false)

			// Run pull, if the command is run in a project directory and allow target env is disabled
			if found && !prj.ProjectManifest().AllowTargetENV() {
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

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
	return cmd
}
