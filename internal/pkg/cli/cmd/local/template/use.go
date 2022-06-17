package template

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	useOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func UseCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `use <repository>/<template>[/<version>]`,
		Short: helpmsg.Read(`local/template/use/short`),
		Long:  helpmsg.Read(`local/template/use/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Local project
			prj, err := d.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions())
			if err != nil {
				return err
			}

			// Parse template argument
			repositoryName, templateId, version, err := parseTemplateArg(args)
			if err != nil {
				return err
			}

			// Repository definition
			manifest := projectState.ProjectManifest()
			repositoryDef, found := manifest.TemplateRepository(repositoryName)
			if !found {
				return fmt.Errorf(`template repository "%s" not found in the "%s"`, repositoryName, manifest.Path())
			}

			// Load template
			template, err := d.Template(model.NewTemplateRef(repositoryDef, templateId, version))
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskUseTemplateOptions(projectState, template.Inputs(), d.Options())
			if err != nil {
				return err
			}

			// Use template
			_, warnings, err := useOp.Run(projectState, template, options, d)

			if len(warnings) > 0 {
				for _, w := range warnings {
					d.Logger().Warnf(w)
				}
			}
			return err
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
		return "", "", "", fmt.Errorf(`please enter one argument - the template you want to use, for example "keboola/my-template/v1"`)
	}
	parts := strings.Split(args[0], "/")
	if len(parts) < 2 || len(parts) > 3 {
		return "", "", "", fmt.Errorf(`the argument must consist of 2 or 3 parts "{repository}/{template}[/{version}]", found "%s"`, args[0])
	}
	repository = parts[0]
	template = parts[1]

	// Version is optional, if it is missing, then default version will be used
	if len(parts) > 2 {
		version = parts[2]
	}

	return
}
