package repository

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func FixPathsCommand(_ dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `fix-paths`,
		Short: helpmsg.Read(`template/repository/fix-paths/short`),
		Long:  helpmsg.Read(`template/repository/fix-paths/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New(`not implemented`)
		},
	}
	return cmd
}
