package generate

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/sources"
)

func SourcesCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `sources`,
		Short: helpmsg.Read(`dbt/generate/sources/short`),
		Long:  helpmsg.Read(`dbt/generate/sources/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check that we are in dbt directory
			if _, _, err := p.LocalDbtProject(cmd.Context()); err != nil {
				return err
			}

			// Ask options
			targetName, err := p.BaseScope().Dialogs().AskTargetName()
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context())
			if err != nil {
				return err
			}

			return sources.Run(cmd.Context(), sources.Options{TargetName: targetName}, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("target-name", "T", "", "target name of the profile")

	return cmd
}
