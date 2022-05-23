package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`local/template/short`),
		Long:  helpmsg.Read(`local/template/long`),
	}
	cmd.AddCommand(
		ListCommand(d),
		UseCommand(d),
		UpgradeCommand(d),
		DeleteCommand(d),
	)
	return cmd
}
