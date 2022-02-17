package remote

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `remote`,
		Short: helpmsg.Read(`remote/short`),
		Long:  helpmsg.Read(`remote/long`),
	}
	cmd.AddCommand(
		CreateCommand(d),
	)
	return cmd
}
