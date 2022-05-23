package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	upgradeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func UpgradeCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upgrade`,
		Short: helpmsg.Read(`local/template/upgrade/short`),
		Long:  helpmsg.Read(`local/template/upgrade/long`),
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

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState, d.Options())
			if err != nil {
				return err
			}

			// Repository definition
			manifest := projectState.ProjectManifest()
			repositoryDef, found := manifest.TemplateRepository(instance.RepositoryName)
			if !found {
				return fmt.Errorf(`template repository "%s" not found in the "%s"`, instance.RepositoryName, manifest.Path())
			}

			// Load template
			version := d.Options().GetString("version")
			template, err := d.Template(model.NewTemplateRef(repositoryDef, instance.TemplateId, version))
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskUpgradeTemplateOptions(projectState, branchKey, *instance, template.Inputs(), d.Options())
			if err != nil {
				return err
			}

			// Use template
			return upgradeOp.Run(projectState, template, options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`instance`, "i", ``, "instance ID of the template to upgrade")
	cmd.Flags().StringP(`version`, "V", ``, "target version, default latest stable version")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	cmd.Flags().StringP(`inputs-file`, "f", ``, "JSON file with inputs values")
	return cmd
}
