package run

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	testOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/run"
)

type Flags struct {
	StorageAPIHost   configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	TestName         string                  `configKey:"test-name" configUsage:"name of a single test to be run"`
	LocalOnly        bool                    `configKey:"local-only" configUsage:"run a local test only"`
	RemoteOnly       bool                    `configKey:"remote-only" configUsage:"run a remote test only"`
	Verbose          bool                    `configKey:"verbose" configUsage:"show details about running tests"`
	TestProjectsFile configmap.Value[string] `configKey:"test-projects-file" configUsage:"file containing projects that could be used for templates"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [template] [version]",
		Short: helpmsg.Read(`template/test/run/short`),
		Long:  helpmsg.Read(`template/test/run/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.LocalCommandScope(cmd.Context(), f.StorageAPIHost, dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			// Get template repository
			repo, _, err := d.LocalTemplateRepository(cmd.Context())
			if err != nil {
				return err
			}

			// Load templates
			templates, err := loadTemplates(cmd, args, d, repo, f)
			if err != nil {
				return err
			}

			// Options
			options := testOp.Options{
				LocalOnly:  f.LocalOnly,
				RemoteOnly: f.RemoteOnly,
				TestName:   f.TestName,
				Verbose:    f.Verbose,
			}

			// Test templates
			errs := errors.NewMultiError()
			for _, tmpl := range templates {
				err := testOp.Run(cmd.Context(), tmpl, options, d)
				if err != nil {
					errs.Append(err)
				}
			}
			return errs.ErrorOrNil()
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func loadTemplates(cmd *cobra.Command, args []string, d dependencies.LocalCommandScope, repo *repository.Repository, f Flags) ([]*template.Template, error) {
	templates := make([]*template.Template, 0)
	if len(args) >= 1 {
		// Optional version argument
		var versionArg string
		if len(args) > 1 {
			versionArg = args[1]
		}
		tmpl, err := d.TemplateForTests(cmd.Context(), model.NewTemplateRef(repo.Definition(), args[0], versionArg), f.TestProjectsFile.Value)
		if err != nil {
			return nil, errors.Errorf(`loading test for template "%s" failed: %w`, args[0], err)
		}
		templates = append(templates, tmpl)
		return templates, nil
	}

	for _, t := range repo.Templates() {
		// Don't test deprecated templates
		if t.Deprecated {
			continue
		}
		v, err := t.DefaultVersionOrErr()
		if err != nil {
			return nil, errors.Errorf(`loading default version for template "%s" failed: %w`, t.ID, err)
		}
		tmpl, err := d.TemplateForTests(cmd.Context(), model.NewTemplateRef(repo.Definition(), t.ID, v.Version.String()), f.TestProjectsFile.Value)
		if err != nil {
			return nil, errors.Errorf(`loading test for template "%s" failed: %w`, t.ID, err)
		}
		templates = append(templates, tmpl)
	}
	return templates, nil
}
