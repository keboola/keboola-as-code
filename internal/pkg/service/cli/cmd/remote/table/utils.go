package table

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
)

// askTable fetches the list of tables in the project, and asks the user to select one.
//
// If `allowCreateNew` is `true`, then it inserts `Create new table` entry into the list.
// If the user selects this entry, they will be asked to enter an ID for the new table
// (which will be checked to be a valid table ID), and if successful, the return value of
// `createNew` will be `true`.
func askTable(d dependencies.ForRemoteCommand, allowCreateNew bool) (tableID keboola.TableID, createNew bool, err error) {
	allTables, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithColumns()).Send(d.CommandCtx())
	if err != nil {
		return keboola.TableID{}, false, err
	}

	opts := []dialog.AskTableOption{}
	if allowCreateNew {
		opts = append(opts, dialog.WithAllowCreateNewTable())
	}

	table, err := d.Dialogs().AskTable(d.Options(), *allTables, opts...)
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
