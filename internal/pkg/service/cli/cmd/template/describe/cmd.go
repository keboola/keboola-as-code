package describe

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	describeOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/describe"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe <template> [version]",
		Short: helpmsg.Read(`template/describe/short`),
		Long:  helpmsg.Read(`template/describe/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			if len(args) < 1 {
				return errors.New(`please enter argument with the template ID you want to use and optionally its version`)
			}

			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Command must be used in template repository
			repo, d, err := p.LocalRepository(cmd.Context(), f.StorageAPIHost, dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			// Optional version argument
			var versionArg string
			if len(args) > 1 {
				versionArg = args[1]
			}

			// Load template
			template, err := d.Template(cmd.Context(), model.NewTemplateRef(repo.Definition(), args[0], versionArg))
			if err != nil {
				return err
			}

			// Describe template
			return describeOp.Run(cmd.Context(), template, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
