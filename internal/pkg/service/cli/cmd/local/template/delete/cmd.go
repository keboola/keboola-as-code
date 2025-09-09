package templatedelete

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"master-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Branch          configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Instance        configmap.Value[string] `configKey:"instance" configShorthand:"i" configUsage:"instance ID of the template to delete"`
	DryRun          configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`local/template/delete/short`),
		Long:  helpmsg.Read(`local/template/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false, f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(cmd.Context(), loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState, f.Branch, f.Instance)
			if err != nil {
				return err
			}

			// Delete template
			options := deleteOp.Options{Branch: branchKey, Instance: instance.InstanceID, DryRun: f.DryRun.Value}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "local-template-delete")

			return deleteOp.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
