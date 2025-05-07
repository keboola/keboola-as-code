package utils

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

// AskTable fetches the list of tables in the project, and asks the user to select one.
//
// If `allowCreateNew` is `true`, then it inserts `Create new table` entry into the list.
// If the user selects this entry, they will be asked to enter an ID for the new table
// (which will be checked to be a valid table ID), and if successful, the return value of
// `createNew` will be `true`.
func AskTable(ctx context.Context, d dependencies.RemoteCommandScope, branchID keboola.BranchID, allowCreateNew bool, id configmap.Value[string]) (tableKey keboola.TableKey, createNew bool, err error) {
	allTables, err := d.KeboolaProjectAPI().ListTablesRequest(branchID, keboola.WithColumns()).Send(ctx)
	if err != nil {
		return keboola.TableKey{}, false, err
	}

	var opts []dialog.AskTableOption
	if allowCreateNew {
		opts = append(opts, dialog.WithAllowCreateNewTable())
	}

	table, err := d.Dialogs().AskTable(*allTables, id, opts...)
	if err != nil {
		return keboola.TableKey{}, false, err
	}

	if table != nil {
		// user selected table
		return table.TableKey, false, nil
	} else {
		// user asked to create new table
		tableID, err := keboola.ParseTableID(d.Dialogs().AskTableID())
		if err != nil {
			return keboola.TableKey{}, false, err
		}

		return keboola.TableKey{BranchID: branchID, TableID: tableID}, true, nil
	}
}
