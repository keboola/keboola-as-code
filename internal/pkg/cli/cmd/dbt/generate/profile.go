package generate

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func ProfileCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profile`,
		Short: helpmsg.Read(`dbt/generate/profile/short`),
		Long:  helpmsg.Read(`dbt/generate/profile/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")

	return cmd
}
