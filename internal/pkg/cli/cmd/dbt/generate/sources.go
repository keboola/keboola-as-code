package generate

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func SourcesCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `sources`,
		Short: helpmsg.Read(`dbt/generate/sources/short`),
		Long:  helpmsg.Read(`dbt/generate/sources/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")

	return cmd
}
