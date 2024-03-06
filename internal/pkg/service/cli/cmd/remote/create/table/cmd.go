package table

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"if command is run outside the project directory"`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Bucket          configmap.Value[string] `configKey:"bucket" configUsage:"bucket ID (required if the tableId argument is empty)"`
	Name            configmap.Value[string] `configKey:"name" configUsage:"name of the table (required if the tableId argument is empty)"`
	Columns         configmap.Value[string] `configKey:"columns" configUsage:"comma-separated list of column names"`
	PrimaryKey      configmap.Value[string] `configKey:"primary-key" configUsage:"columns used as primary key, comma-separated"`
	ColumnsFrom     configmap.Value[string] `configKey:"columns-from" configUsage:"the path to the table definition file in json"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table [table]",
		Short: helpmsg.Read(`remote/create/table/short`),
		Long:  helpmsg.Read(`remote/create/table/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken, dependencies.WithoutMasterToken())
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
			if len(args) == 0 && !f.Bucket.IsSet() {
				// Get buckets list for dialog select only if needed
				allBucketsPtr, err := d.KeboolaProjectAPI().ListBucketsRequest(branch.ID).Send(cmd.Context())
				if err != nil {
					return err
				}
				allBuckets = *allBucketsPtr
			}
			opts, err := AskCreateTable(args, branch.BranchKey, allBuckets, d.Dialogs(), f)
			if err != nil {
				return err
			}

			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-create-table")

			return table.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
