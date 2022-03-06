package repository

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func ValidateCommand(_ dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `validate`,
		Short: helpmsg.Read(`template/repository/validate/short`),
		Long:  helpmsg.Read(`template/repository/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}
	return cmd
}
