package generate

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func EnvCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `env`,
		Short: helpmsg.Read(`dbt/generate/env/short`),
		Long:  helpmsg.Read(`dbt/generate/env/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")

	return cmd
}
