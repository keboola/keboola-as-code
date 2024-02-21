package use

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	useOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	Branch     configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Instance   configmap.Value[string] `configKey:"instance" configShorthand:"i" configUsage:"instance ID of the template to upgrade"`
	Version    configmap.Value[string] `configKey:"version" configShorthand:"V" configUsage:"target version, default latest stable version"`
	DryRun     configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
	InputsFile configmap.Value[string] `configKey:"inputs-file" configShorthand:"f" configUsage:"JSON file with inputs values"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `use <repository>/<template>[/<version>]`,
		Short: helpmsg.Read(`local/template/use/short`),
		Long:  helpmsg.Read(`local/template/use/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// flags
			f := Flags{}
			if err = configmap.Bind(utils.GetBindConfig(cmd.Flags(), args), &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
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

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "target branch ID or name")
	cmd.Flags().StringP(`instance-name`, "n", ``, "name of new template instance")
	cmd.Flags().StringP(`inputs-file`, "f", ``, "JSON file with inputs values")
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

	return
}
