package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`local/template/short`),
		Long:  helpmsg.Read(`local/template/long`),
	}
	cmd.AddCommand(
		ListCommand(p),
		UseCommand(p),
		UpgradeCommand(p),
		RenameCommand(p),
		DeleteCommand(p),
	)
	return cmd
}
