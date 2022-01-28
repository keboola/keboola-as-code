package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	useOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/use"
)

func UseCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "use",
		Short: helpmsg.Read(`template/use/short`),
		Long:  helpmsg.Read(`template/use/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Require template dir
			if _, err := d.TemplateDir(); err != nil {
				return err
			}

			// Require project dir
			if _, err := d.ProjectDir(); err != nil {
				return err
			}

			// Load state
			projectState, err := d.ProjectState(useOp.LoadProjectOptions())
			if err != nil {
				return err
			}

			var defaultBranch *model.Branch
			for _, branch := range projectState.LocalObjects().Branches() {
				if branch.IsDefault {
					defaultBranch = branch
				}
			}

			// Options
			options := useOp.Options{
				TargetBranch: defaultBranch.Id,
			}

			// Create template
			return useOp.Run(options, d)
		},
	}
	return cmd
}
