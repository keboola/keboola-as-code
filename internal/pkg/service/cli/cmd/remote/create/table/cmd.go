package table

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

type Flags struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"if command is run outside the project directory"`
	Bucket         string `configKey:"bucket" configUsage:"bucket ID (required if the tableId argument is empty)"`
	Name           string `configKey:"name" configUsage:"name of the table (required if the tableId argument is empty)"`
	Columns        string `configKey:"columns" configUsage:"comma-separated list of column names"`
	PrimaryKey     string `configKey:"primary-key" configUsage:"columns used as primary key, comma-separated"`
}

func Command(p dependencies.Provider) *cobra.Command {
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

			// Options
			var allBuckets []*keboola.Bucket
			if len(args) == 0 && !d.Options().IsSet("bucket") {
				// Get buckets list for dialog select only if needed
				allBucketsPtr, err := d.KeboolaProjectAPI().ListBucketsRequest().Send(cmd.Context())
				if err != nil {
					return err
				}
				allBuckets = *allBucketsPtr
			}
			opts, err := d.Dialogs().AskCreateTable(args, allBuckets)
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-table")

			return table.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
