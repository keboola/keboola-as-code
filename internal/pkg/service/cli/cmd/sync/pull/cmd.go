package pull

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/gitbranch"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost         configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken        configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Force                  configmap.Value[bool]   `configKey:"force" configUsage:"ignore invalid local state"`
	DryRun                 configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
	CleanupRenameConflicts configmap.Value[bool]   `configKey:"cleanup-rename-conflicts" configUsage:"enable cleanup mode for rename conflicts (removes conflicting destinations)"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: helpmsg.Read(`sync/pull/short`),
		Long:  helpmsg.Read(`sync/pull/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Command must be used in project directory
			_, _, err := p.BaseScope().FsInfo().ProjectDir(cmd.Context())
			if err != nil {
				return err
			}

			f := Flags{}
			if err = p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Set KBC_BRANCH_ID from git-branching BEFORE RemoteCommandScope
			// RemoteCommandScope calls LocalProject() internally to verify project ID,
			// which caches the manifest. The branch ID must be set before that.
			logger := p.BaseScope().Logger()
			_, err = gitbranch.SetBranchIDFromGitBranching(cmd.Context(), p.BaseScope().Fs(), p.BaseScope().EnvironmentMap(), logger)
			if err != nil {
				return err
			}

			// Authentication
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get local project
			force := f.Force.Value
			prj, _, err := d.LocalProject(cmd.Context(), force)
			if err != nil {
				if !force && errors.As(err, &project.InvalidManifestError{}) {
					logger.Info(cmd.Context(), "")
					logger.Info(cmd.Context(), "Use --force to override the invalid local state.")
				}
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.PullOptions(force), d)
			if err != nil {
				if !force && errors.As(err, &loadState.InvalidLocalStateError{}) {
					logger.Info(cmd.Context(), "")
					logger.Info(cmd.Context(), "Use --force to override the invalid local state.")
				}
				return err
			}

			// Options
			options := pull.Options{
				DryRun:                 f.DryRun.Value,
				LogUntrackedPaths:      true,
				CleanupRenameConflicts: f.CleanupRenameConflicts.Value,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "sync-pull")

			// Pull
			return pull.Run(cmd.Context(), projectState, options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
