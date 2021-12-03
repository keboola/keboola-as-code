package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func EditCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit",
		Short: helpmsg.Read(`template/edit/short`),
		Long:  helpmsg.Read(`template/edit/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}
	return cmd
}
