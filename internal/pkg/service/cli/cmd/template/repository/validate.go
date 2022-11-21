package repository

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func ValidateCommand(_ dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `validate`,
		Short: helpmsg.Read(`template/repository/validate/short`),
		Long:  helpmsg.Read(`template/repository/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New(`not implemented`)
		},
	}
	return cmd
}
