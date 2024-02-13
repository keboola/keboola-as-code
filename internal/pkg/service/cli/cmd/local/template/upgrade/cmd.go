package upgrade

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	upgradeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	Branch     string `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Instance   string `configKey:"instance" configShorthand:"i" configUsage:"instance ID of the template to upgrade"`
	Version    string `configKey:"version" configShorthand:"V" configUsage:"target version, default latest stable version"`
	DryRun     bool   `configKey:"dry-run" configUsage:"print what needs to be done"`
	InputsFile string `configKey:"inputs-file" configShorthand:"f" configUsage:"JSON file with inputs values"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `upgrade`,
		Short: helpmsg.Read(`local/template/upgrade/short`),
		Long:  helpmsg.Read(`local/template/upgrade/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState)
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
			template, err := d.Template(cmd.Context(), model.NewTemplateRef(repositoryDef, instance.TemplateID, version))
			if err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskUpgradeTemplateOptions(cmd.Context(), d, projectState.State(), branchKey, *instance, template.Inputs())
			if err != nil {
				return err
			}

			// Use template
			opResult, err := upgradeOp.Run(cmd.Context(), projectState, template, options, d)
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

	cliconfig.MustGenerateFlags(Flags{}, cmd.Flags())

	return cmd
}
