package table

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
)

// AskTable fetches the list of tables in the project, and asks the user to select one.
//
// If `allowCreateNew` is `true`, then it inserts `Create new table` entry into the list.
// If the user selects this entry, they will be asked to enter an ID for the new table
// (which will be checked to be a valid table ID), and if successful, the return value of
// `createNew` will be `true`.
func AskTable(ctx context.Context, d dependencies.RemoteCommandScope, allowCreateNew bool) (tableID keboola.TableID, createNew bool, err error) {
	allTables, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithColumns()).Send(ctx)
	if err != nil {
		return keboola.TableID{}, false, err
	}

	var opts []dialog.AskTableOption
	if allowCreateNew {
		opts = append(opts, dialog.WithAllowCreateNewTable())
	}

	table, err := d.Dialogs().AskTable(*allTables, opts...)
	if err != nil {
		return keboola.TableID{}, false, err
	}

	if table != nil {
		// user selected table
		return table.ID, false, nil
	} else {
		// user asked to create new table
		tableID, err := keboola.ParseTableID(d.Dialogs().AskTableID())
		if err != nil {
			return keboola.TableID{}, false, err
		}

		return tableID, true, nil
	}
}

func ParseUnloadOptions(options *options.Options, tableID keboola.TableID) (unload.Options, error) {
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
			whereFilter, err := preview.ParseWhereFilter(s)
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
			columnOrder, err := preview.ParseColumnOrder(s)
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
