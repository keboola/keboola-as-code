package table

import (
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	common "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

func UnloadCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `unload [table]`,
		Short: helpmsg.Read(`remote/table/unload/short`),
		Long:  helpmsg.Read(`remote/table/unload/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Ask for host and token if needed
			baseDeps := p.BaseDependencies()
			if err := baseDeps.Dialogs().AskHostAndToken(baseDeps); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.DependenciesForRemoteCommand(common.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(d.CommandCtx(), time.Now(), &cmdErr, "remote-table-unload")

			var tableID keboola.TableID
			if len(args) == 0 {
				allTables, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithColumns()).Send(d.CommandCtx())
				if err != nil {
					return err
				}

				table, err := d.Dialogs().AskTable(d.Options(), *allTables)
				if err != nil {
					return err
				}
				tableID = table.ID
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

			return unload.Run(d.CommandCtx(), o, d)
		},
	}

	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().String("changed-since", "", "only export rows imported after this date")
	cmd.Flags().String("changed-until", "", "only export rows imported before this date")
	cmd.Flags().StringSlice("columns", []string{}, "comma-separated list of columns to export")
	cmd.Flags().Uint("limit", 100, "limit the number of exported rows")
	cmd.Flags().String("where", "", "filter columns by value")
	cmd.Flags().String("order", "", "order by one or more columns")
	cmd.Flags().String("format", "csv", "output format (json/csv)")
	cmd.Flags().Bool("async", false, "do not wait for unload to finish")
	cmd.Flags().String("timeout", "5m", "how long to wait for job to finish")

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
			whereFilter, err := parseWhereFilter(s)
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
			columnOrder, err := parseColumnOrder(s)
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
