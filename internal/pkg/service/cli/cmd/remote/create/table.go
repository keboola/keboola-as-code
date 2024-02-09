package create

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

type TableFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"if command is run outside the project directory"`
	Bucket         string `mapstructure:"bucket" usage:"bucket ID (required if the tableId argument is empty)"`
	Name           string `mapstructure:"name" usage:"name of the table (required if the tableId argument is empty)"`
	Columns        string `mapstructure:"columns" usage:"comma-separated list of column names"`
	PrimaryKey     string `mapstructure:"primary-key" usage:"columns used as primary key, comma-separated"`
}

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

	cliconfig.MustGenerateFlags(TableFlags{}, cmd.Flags())

	return cmd
}
