package use

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	useOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Branch          configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"target branch ID or name"`
	InstanceName    configmap.Value[string] `configKey:"instance-name" configShorthand:"n" configUsage:"name of new template instance"`
	InputsFile      configmap.Value[string] `configKey:"inputs-file" configShorthand:"f" configUsage:"JSON file with inputs values"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `use <repository>/<template>[/<version>]`,
		Short: helpmsg.Read(`local/template/use/short`),
		Long:  helpmsg.Read(`local/template/use/long`),
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

			// Parse template argument
			repositoryName, templateID, version, err := parseTemplateArg(args)
			if err != nil {
				return err
			}

			// Repository definition
			manifest := projectState.ProjectManifest()
			repositoryDef, found := manifest.TemplateRepository(repositoryName)
			if !found {
				return errors.Errorf(`template repository "%s" not found in the "%s"`, repositoryName, manifest.Path())
			}

			// Load template
			template, err := d.Template(cmd.Context(), model.NewTemplateRef(repositoryDef, templateID, version))
			if err != nil {
				return err
			}

			// Options
			options, err := AskUseTemplateOptions(cmd.Context(), d.Dialogs(), projectState, template.Inputs(), f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "local-template-use")

			// Use template
			opResult, err := useOp.Run(cmd.Context(), projectState, template, options, d)
			if err != nil {
				return err
			}

			if len(opResult.Warnings) > 0 {
				for _, w := range opResult.Warnings {
					d.Logger().Warn(cmd.Context(), w)
				}
			}

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func parseTemplateArg(args []string) (repository string, template string, version string, err error) {
	if len(args) != 1 {
		return "", "", "", errors.New(`please enter one argument - the template you want to use, for example "keboola/my-template/v1"`)
	}
	parts := strings.Split(args[0], "/")
	if len(parts) < 2 || len(parts) > 3 {
		return "", "", "", errors.Errorf(`the argument must consist of 2 or 3 parts "{repository}/{template}[/{version}]", found "%s"`, args[0])
	}
	repository = parts[0]
	template = parts[1]

	// Version is optional, if it is missing, then default version will be used
	if len(parts) > 2 {
		version = parts[2]
	}

	return repository, template, version, err
}
