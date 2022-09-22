package workspace

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func DeleteCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`remote/workspace/delete/short`),
		Long:  helpmsg.Read(`remote/workspace/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")

	return cmd
}
