package utils

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
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
			whereFilter, err := ParseWhereFilter(s)
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
			columnOrder, err := ParseColumnOrder(s)
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

func ParseWhereFilter(s string) (preview.WhereFilter, error) {
	m := regexpcache.MustCompile(`^(\w+)(=|!=|>=|<=)([^=!<>]*)$`).FindStringSubmatch(s)
	if m == nil {
		return preview.WhereFilter{}, errors.Errorf(`invalid where filter "%s"`, s)
	}

	column := m[1]
	operator, err := keboola.ParseCompareOp(m[2])
	if err != nil {
		return preview.WhereFilter{}, err
	}
	values := strings.Split(m[3], ",")

	return preview.WhereFilter{
		Column:   column,
		Operator: operator,
		Values:   values,
	}, nil
}

func ParseColumnOrder(s string) (preview.ColumnOrder, error) {
	m := regexpcache.MustCompile(`(\w+)(?:=(asc|desc))?`).FindStringSubmatch(s)
	if m == nil {
		return preview.ColumnOrder{}, errors.Errorf(`invalid column order "%s"`, s)
	}

	column := m[1]
	orderString := m[2]
	if len(orderString) == 0 {
		orderString = "asc"
	}
	order, err := keboola.ParseColumnOrder(orderString)
	if err != nil {
		return preview.ColumnOrder{}, err
	}

	return preview.ColumnOrder{Column: column, Order: order}, nil
}
