package unload

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

type Flags struct {
	StorageAPIHost string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	ChangedSince   string   `mapstructure:"changed-since" usage:"only export rows imported after this date"`
	ChangedUntil   string   `mapstructure:"changed-until" usage:"only export rows imported before this date"`
	Columns        []string `mapstructure:"columns" usage:"comma-separated list of columns to export"`
	Limit          uint     `mapstructure:"limit" usage:"limit the number of exported rows"`
	Where          string   `mapstructure:"where" usage:"filter columns by value"`
	Order          string   `mapstructure:"order" usage:"order by one or more columns"`
	Format         string   `mapstructure:"format" usage:"output format (json/csv)"`
	Async          bool     `mapstructure:"async" usage:"do not wait for unload to finish"`
	Timeout        string   `mapstructure:"timeout" usage:"how long to wait for job to finish"`
}

func DefaultFlags() *Flags {
	return &Flags{
		Limit:   0,
		Columns: []string{},
		Format:  "csv",
		Timeout: "5m",
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `unload [table]`,
		Short: helpmsg.Read(`remote/table/unload/short`),
		Long:  helpmsg.Read(`remote/table/unload/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Ask options
			var tableID keboola.TableID
			if len(args) == 0 {
				tableID, _, err = table.AskTable(cmd.Context(), d, false)
				if err != nil {
					return err
				}
			} else {
				id, err := keboola.ParseTableID(args[0])
				if err != nil {
					return err
				}
				tableID = id
			}

			o, err := table.ParseUnloadOptions(d.Options(), tableID)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-unload")

			_, err = unload.Run(cmd.Context(), o, d)
			return err
		},
	}

	cliconfig.MustGenerateFlags(DefaultFlags(), cmd.Flags())

	return cmd
}
