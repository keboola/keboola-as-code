package push

import (
	"github.com/spf13/cobra"

	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Force           configmap.Value[bool]   `configKey:"force" configUsage:"enable deleting of remote objects"`
	DryRun          configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
	Encrypt         configmap.Value[bool]   `configKey:"encrypt" configUsage:"encrypt unencrypted values before push"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `push ["change description"]`,
		Short: helpmsg.Read(`sync/push/short`),
		Long:  helpmsg.Read(`sync/push/long`),
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

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get local project.
			// Use ignoreErrors=true so that an inconsistent manifest (e.g. a scheduler
			// whose orchestrator parent was never pulled) does not block push.
			// SetRecords() deletes any orphaned records, so no record is left with an
			// unresolved parent path. A warning is logged by manifest.Load() in this case.
			// NOTE: if the orphaned configs still exist in remote, running kbc push --force
			// will schedule them for remote deletion (they are absent from local state).
			hint := "Orphaned records are excluded from the push. Running `kbc push --force` may delete them from remote. " +
				"Run `kbc pull --force` to reset local state."
			if f.Force.Value {
				hint = "Orphaned records are excluded from the push. " +
					"With --force, these configs will be deleted from remote if they still exist there. " +
					"Run `kbc pull --force` to reset local state first."
			}
			ctx := projectManifest.WithLoadHint(cmd.Context(), hint)
			prj, _, err := d.LocalProject(ctx, true)
			if err != nil {
				return err
			}

			// Snapshot IsChanged() before LoadState to capture only orphan-cleanup changes.
			// LoadState and push.Run do not modify the manifest (push is remote-only),
			// so any post-snapshot IsChanged() would only be from orphan cleanup.
			orphansCleaned := prj.ProjectManifest().IsChanged()

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.PushOptions(), d)
			if err != nil {
				return err
			}

			// If orphaned records were dropped during manifest load, the in-memory state
			// diverges from the manifest file on disk. Save the cleaned manifest now so
			// the warning does not recur on every subsequent push/diff invocation.
			// Skip when --dry-run: that flag promises no local side effects.
			if orphansCleaned && !f.DryRun.Value {
				if _, err = saveManifest.Run(cmd.Context(), projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
					return err
				}
			}

			// Change description - optional arg
			changeDescription := "Updated from #KeboolaCLI"
			if len(args) > 0 {
				changeDescription = args[0]
			}

			// Options
			options := push.Options{
				Encrypt:           f.Encrypt.Value,
				DryRun:            f.DryRun.Value,
				AllowRemoteDelete: f.Force.Value,
				LogUntrackedPaths: true,
				ChangeDescription: changeDescription,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "sync-push")

			// Push
			return push.Run(cmd.Context(), projectState, options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
