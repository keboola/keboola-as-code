package repository

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/init"
)

func InitCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `init`,
		Short: helpmsg.Read(`template/repository/init/short`),
		Long:  helpmsg.Read(`template/repository/init/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			// Require empty dir
			if _, err := d.EmptyDir(); err != nil {
				return err
			}

			// Init repository
			return initOp.Run(d.CommandCtx(), d)
		},
	}
	return cmd
}
