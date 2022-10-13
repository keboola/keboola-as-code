package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	upgradeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func UpgradeCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upgrade`,
		Short: helpmsg.Read(`local/template/upgrade/short`),
		Long:  helpmsg.Read(`local/template/upgrade/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
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
				return errors.Errorf(`template repository "%s" not found in the "%s"`, instance.RepositoryName, manifest.Path())
			}

			// Load template
			version := d.Options().GetString("version")
			template, err := d.Template(d.CommandCtx(), model.NewTemplateRef(repositoryDef, instance.TemplateId, version))
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskUpgradeTemplateOptions(d, projectState.State(), branchKey, *instance, template.Inputs())
			if err != nil {
				return err
			}

			// Use template
			warnings, err := upgradeOp.Run(d.CommandCtx(), projectState, template, options, d)
			if err != nil {
				return err
			}

			if len(warnings) > 0 {
				for _, w := range warnings {
					d.Logger().Warnf(w)
				}
			}

			return nil
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
