package repository

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func FixPathsCommand(_ dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `fix-paths`,
		Short: helpmsg.Read(`template/repository/fix-paths/short`),
		Long:  helpmsg.Read(`template/repository/fix-paths/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}
	return cmd
}
