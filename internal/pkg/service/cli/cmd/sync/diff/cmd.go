package diff

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/printdiff"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Details         configmap.Value[bool]   `configKey:"details" configUsage:"print changed fields"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: helpmsg.Read(`sync/diff/short`),
		Long:  helpmsg.Read(`sync/diff/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in project directory
			_, _, err := p.BaseScope().FsInfo().ProjectDir(cmd.Context())
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Get local project
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.DiffOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := printdiff.Options{
				PrintDetails:      f.Details.Value,
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "sync-diff")

			// Print diff
			results, err := printdiff.Run(cmd.Context(), projectState, options, d)
			if err != nil {
				return err
			}

			// Print info about --details flag
			if !options.PrintDetails && results.HasNotEqualResult {
				logger := d.Logger()
				logger.Info(cmd.Context(), "")
				logger.Info(cmd.Context(), `Use --details flag to list the changed fields.`)
			}
			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
