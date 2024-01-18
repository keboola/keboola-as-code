package create

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

func TableCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table [table]",
		Short: helpmsg.Read(`remote/create/table/short`),
		Long:  helpmsg.Read(`remote/create/table/long`),
		Args:  cobra.MaximumNArgs(1),
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
			var allBuckets []*keboola.Bucket
			if len(args) == 0 && !d.Options().IsSet("bucket") {
				// Get buckets list for dialog select only if needed
				allBucketsPtr, err := d.KeboolaProjectAPI().ListBucketsRequest(branch.ID).Send(cmd.Context())
				if err != nil {
					return err
				}
				allBuckets = *allBucketsPtr
			}
			opts, err := d.Dialogs().AskCreateTable(args, branch.BranchKey, allBuckets)
			if err != nil {
				return err
			}

			// fmt.Println(opts.CreateTableRequest)
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-table")

			return table.Run(cmd.Context(), opts, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP("storage-api-host", "H", "", "if command is run outside the project directory")
	cmd.Flags().String("bucket", "", "bucket ID (required if the tableId argument is empty)")
	cmd.Flags().String("name", "", "name of the table (required if the tableId argument is empty)")
	cmd.Flags().String("columns", "", "comma-separated list of column names")
	cmd.Flags().String("primary-key", "", "columns used as primary key, comma-separated")
	cmd.Flags().String("columns-from", "", "columns used as primary key, comma-separated")

	return cmd
}
