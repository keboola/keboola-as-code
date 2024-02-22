package table

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

func AskCreateTable(args []string, branchKey keboola.BranchKey, allBuckets []*keboola.Bucket, d *dialog.Dialogs, f Flags) (table.Options, error) {
	opts := table.Options{}

	if len(args) == 1 {
		tableID, err := keboola.ParseTableID(args[0])
		if err != nil {
			return opts, err
		}
		opts.BucketKey = keboola.BucketKey{BranchID: branchKey.ID, BucketID: tableID.BucketID}
		opts.Name = tableID.TableName
	} else {
		bucketID, err := askBucketID(allBuckets, d, f.Bucket)
		if err != nil {
			return opts, err
		}

		opts.BucketKey = keboola.BucketKey{BranchID: branchKey.ID, BucketID: bucketID}

		name := f.Name.Value
		if !f.Name.IsSet() {
			name, _ = d.Ask(&prompt.Question{
				Label:       "Table name",
				Description: "Enter the table name.",
			})
		}
		opts.Name = name
	}

	columnsStr := f.Columns.Value
	if !f.Columns.IsSet() {
		columnsStr, _ = d.Ask(&prompt.Question{
			Label:       "Columns",
			Description: "Enter a comma-separated list of column names.",
		})
	}
	opts.Columns = strings.Split(strings.TrimSpace(columnsStr), ",")

	if f.PrimaryKey.IsSet() {
		opts.PrimaryKey = strings.Split(strings.TrimSpace(f.PrimaryKey.Value), ",")
	} else {
		primaryKey, _ := d.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: opts.Columns,
		})
		opts.PrimaryKey = primaryKey
	}

	return opts, nil
}

func askBucketID(all []*keboola.Bucket, d *dialog.Dialogs, bucket configmap.Value[string]) (keboola.BucketID, error) {
	if bucket.IsSet() {
		return keboola.ParseBucketID(bucket.Value)
	}

	selectOpts := make([]string, 0)
	for _, b := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.DisplayName, b.BucketID.String()))
	}
	if index, ok := d.SelectIndex(&prompt.SelectIndex{
		Label:   "Select a bucket",
		Options: selectOpts,
	}); ok {
		return all[index].BucketID, nil
	}

	return keboola.BucketID{}, errors.New(`please specify bucket`)
}
