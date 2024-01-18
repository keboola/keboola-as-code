package dialog

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

func (p *Dialogs) AskCreateTable(args []string, branchKey keboola.BranchKey, allBuckets []*keboola.Bucket) (table.Options, error) {
	opts := table.Options{}

	if len(args) == 1 {
		tableID, err := keboola.ParseTableID(args[0])
		if err != nil {
			return opts, err
		}
		opts.BucketKey = keboola.BucketKey{BranchID: branchKey.ID, BucketID: tableID.BucketID}
		opts.Name = tableID.TableName
	} else {
		bucketID, err := p.AskBucketID(allBuckets)
		if err != nil {
			return opts, err
		}

		opts.BucketKey = keboola.BucketKey{BranchID: branchKey.ID, BucketID: bucketID}

		name := p.options.GetString(`name`)
		if !p.options.IsSet(`name`) {
			name, _ = p.Ask(&prompt.Question{
				Label:       "Table name",
				Description: "Enter the table name.",
			})
		}
		opts.Name = name
	}

	columnsStr := p.options.GetString(`columns`)
	if !p.options.IsSet(`columns`) {
		columnsStr, _ = p.Ask(&prompt.Question{
			Label:       "Columns",
			Description: "Enter a comma-separated list of column names.",
		})
	}
	opts.Columns = strings.Split(strings.TrimSpace(columnsStr), ",")

	if p.options.IsSet(`primary-key`) {
		opts.PrimaryKey = strings.Split(strings.TrimSpace(p.options.GetString(`primary-key`)), ",")
	} else {
		primaryKey, _ := p.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: opts.Columns,
		})
		opts.PrimaryKey = primaryKey
	}

	fileName := p.options.GetString("columns-from")
	if p.options.IsSet("columns-from") {
		as, err := parseJsonInput(fileName)
		if err != nil {
			return table.Options{}, err
		}
		opts.CreateTableRequest = *as
	}

	return opts, nil
}

func parseJsonInput(filePath string) (*keboola.CreateTableRequest, error) {
	//
	dataFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var result *keboola.CreateTableRequest

	err = json.Unmarshal(dataFile, &result)
	if err != nil {
		return nil, err
	}

	return result, err
}
