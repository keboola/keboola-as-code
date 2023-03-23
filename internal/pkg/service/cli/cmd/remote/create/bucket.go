package create

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func BucketCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bucket",
		Short: helpmsg.Read(`remote/create/bucket/short`),
		Long:  helpmsg.Read(`remote/create/bucket/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			_, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP("storage-api-host", "H", "", "if command is run outside the project directory")
	cmd.Flags().String("description", "", "bucket description")
	cmd.Flags().String("display-name", "", "display name for the UI")
	cmd.Flags().String("name", "", "name of the bucket")
	cmd.Flags().String("stage", "", "stage, allowed values: in, out")
	return cmd
}
