package template

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	useOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

func UseCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `use <repository>/<template>/<version>`,
		Short: helpmsg.Read(`local/template/use/short`),
		Long:  helpmsg.Read(`local/template/use/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Parse template argument
			repositoryName, templateId, versionStr, err := parseTemplateArg(args)
			if err != nil {
				return err
			}

			// Load project
			project, err := d.LocalProject()
			if err != nil {
				return err
			}

			// Repository definition
			manifest := project.ProjectManifest()
			repositoryDef, found := manifest.TemplateRepository(repositoryName)
			if !found {
				return fmt.Errorf(`template repository "%s" not found in the "%s"`, repositoryName, manifest.Path())
			}

			// Template definition
			templateDef := model.TemplateRef{
				Id:         templateId,
				Version:    versionStr,
				Repository: repositoryDef,
			}

			// Template
			template, err := d.Template(templateDef)
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskUseTemplateOptions(template.Inputs(), d, useOp.LoadProjectOptions())
			if err != nil {
				return err
			}

			// Create template
			return useOp.Run(template, options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "target branch ID or name")
	cmd.Flags().StringP(`inputs-file`, "f", ``, "JSON file with inputs values")
	return cmd
}

func parseTemplateArg(args []string) (repository string, template string, version string, err error) {
	if len(args) != 1 {
		return "", "", "", fmt.Errorf(`please enter one argument - the template you want to use, for example "keboola/my-template/v1"`)
	}
	parts := strings.Split(args[0], "/")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf(`the argument must consist of 3 parts "{repository}/{template}/{version}", found "%s"`, args[0])
	}
	repository = parts[0]
	template = parts[1]
	version = parts[2]
	return
}
