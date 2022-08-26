package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	testOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test"
)

func TestCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test [template] [version]",
		Short: helpmsg.Read(`template/test/short`),
		Long:  helpmsg.Read(`template/test/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Get template repository
			repo, err := d.TemplateRepository(model.TemplateRepository{Type: model.RepositoryTypeWorkingDir, Name: "keboola"}, nil)
			if err != nil {
				return err
			}

			// Load templates
			templates := make([]*template.Template, 0)
			if len(args) >= 1 {
				// Optional version argument
				var versionArg string
				if len(args) > 1 {
					versionArg = args[1]
				}
				tmpl, err := d.Template(model.NewTemplateRef(repo.Ref(), args[0], versionArg))
				if err != nil {
					return err
				}
				templates = append(templates, tmpl)
			} else {
				for _, t := range repo.Templates() {
					v, err := t.DefaultVersionOrErr()
					if err != nil {
						return err
					}
					tmpl, err := d.Template(model.NewTemplateRef(repo.Ref(), t.Id, v.Version.String()))
					if err != nil {
						return err
					}
					templates = append(templates, tmpl)
				}
			}

			// Options
			options := testOp.Options{
				LocalOnly:  d.Options().GetBool("local-only"),
				RemoteOnly: d.Options().GetBool("remote-only"),
				TestName:   d.Options().GetString("test-name"),
				Verbose:    d.Options().GetBool("verbose"),
			}

			// Test templates
			for _, tmpl := range templates {
				err := testOp.Run(tmpl, options, d)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().String("test-name", "", "name of a single test to be run")
	cmd.Flags().Bool("local-only", false, "run a local test only")
	cmd.Flags().Bool("remote-only", false, "run a remote test only")

	return cmd
}
