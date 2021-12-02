package repo

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func ValidateCommand(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `validate`,
		Short: helpmsg.Read(`template/repo/validate/short`),
		Long:  helpmsg.Read(`template/repo/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}
	return cmd
}
