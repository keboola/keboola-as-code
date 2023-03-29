package dialog

import (
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

type createTableDeps interface {
	Options() *options.Options
}

func (p *Dialogs) AskCreateTable(args []string, d createTableDeps, allBuckets []*keboola.Bucket) (table.Options, error) {
	opts := table.Options{}

	if len(args) == 1 {
		tableID, err := keboola.ParseTableID(args[0])
		if err != nil {
			return opts, err
		}
		opts.BucketID = tableID.BucketID
		opts.Name = tableID.TableName
	} else {
		bucketID, err := p.AskBucketID(d.Options(), allBuckets)
		if err != nil {
			return opts, err
		}
		opts.BucketID = bucketID

		name := d.Options().GetString(`name`)
		if !d.Options().IsSet(`name`) {
			name, _ = p.Ask(&prompt.Question{
				Label:       "Table name",
				Description: "Enter the table name.",
			})
		}
		opts.Name = name
	}

	columnsStr := d.Options().GetString(`columns`)
	if !d.Options().IsSet(`columns`) {
		columnsStr, _ = p.Ask(&prompt.Question{
			Label:       "Columns",
			Description: "Enter a comma-separated list of column names.",
		})
	}
	opts.Columns = strings.Split(strings.TrimSpace(columnsStr), ",")

	if d.Options().IsSet(`primary-key`) {
		opts.PrimaryKey = strings.Split(strings.TrimSpace(d.Options().GetString(`primary-key`)), ",")
	} else {
		primaryKey, _ := p.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: opts.Columns,
		})
		opts.PrimaryKey = primaryKey
	}

	return opts, nil
}
