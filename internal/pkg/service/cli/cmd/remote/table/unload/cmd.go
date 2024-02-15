package unload

import (
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

type Flags struct {
	StorageAPIHost string   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	ChangedSince   string   `configKey:"changed-since" configUsage:"only export rows imported after this date"`
	ChangedUntil   string   `configKey:"changed-until" configUsage:"only export rows imported before this date"`
	Columns        []string `configKey:"columns" configUsage:"comma-separated list of columns to export"`
	Limit          uint     `configKey:"limit" configUsage:"limit the number of exported rows"`
	Where          string   `configKey:"where" configUsage:"filter columns by value"`
	Order          string   `configKey:"order" configUsage:"order by one or more columns"`
	Format         string   `configKey:"format" configUsage:"output format (json/csv)"`
	Async          bool     `configKey:"async" configUsage:"do not wait for unload to finish"`
	Timeout        string   `configKey:"timeout" configUsage:"how long to wait for job to finish"`
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
				tableID, _, err = utils.AskTable(cmd.Context(), d, false)
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

			o, err := parseUnloadOptions(d.Options(), tableID)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-unload")

			_, err = unload.Run(cmd.Context(), o, d)
			return err
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func parseUnloadOptions(options *options.Options, tableID keboola.TableID) (unload.Options, error) {
	o := unload.Options{TableID: tableID}

	o.ChangedSince = options.GetString("changed-since")
	o.ChangedUntil = options.GetString("changed-until")
	o.Columns = options.GetStringSlice("columns")
	o.Limit = options.GetUint("limit")
	o.Async = options.GetBool("async")

	e := errors.NewMultiError()

	timeout, err := time.ParseDuration(options.GetString("timeout"))
	if err != nil {
		e.Append(err)
	}
	o.Timeout = timeout

	whereString := options.GetString("where")
	if len(whereString) > 0 {
		for _, s := range strings.Split(whereString, ";") {
			whereFilter, err := utils.ParseWhereFilter(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.WhereFilters = append(o.WhereFilters, whereFilter)
		}
	}

	orderString := options.GetString("order")
	if len(orderString) > 0 {
		for _, s := range strings.Split(orderString, ",") {
			columnOrder, err := utils.ParseColumnOrder(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.Order = append(o.Order, columnOrder)
		}
	}

	format, err := unload.ParseFormat(options.GetString("format"))
	if err != nil {
		e.Append(err)
	}
	o.Format = format

	if err := e.ErrorOrNil(); err != nil {
		return unload.Options{}, err
	}

	return o, nil
}
