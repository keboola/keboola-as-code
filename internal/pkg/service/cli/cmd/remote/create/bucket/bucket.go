package bucket

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/bucket"
)

func BucketCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bucket",
		Short: helpmsg.Read(`remote/create/bucket/short`),
		Long:  helpmsg.Read(`remote/create/bucket/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Get default branch
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot get default branch: %w", err)
			}

			// Options
			opts, err := d.Dialogs().AskCreateBucket(branch.BranchKey)
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-bucket")

			return bucket.Run(cmd.Context(), opts, d)
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
