package template

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func EditorCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "editor",
		Short: helpmsg.Read(`template/editor/short`),
		Long:  helpmsg.Read(`template/editor/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf(`not implemented`)
		},
	}
	return cmd
}
