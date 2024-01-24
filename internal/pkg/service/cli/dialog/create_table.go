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
		opts.CreateTableRequest.Name = tableID.TableName
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
		opts.CreateTableRequest.Name = name
	}

	columnsStr := p.options.GetString(`columns`)
	if !p.options.IsSet(`columns`) {
		columnsStr, _ = p.Ask(&prompt.Question{
			Label:       "Columns",
			Description: "Enter a comma-separated list of column names.",
		})
	}
	colNames := strings.Split(strings.TrimSpace(columnsStr), ",")

	if p.options.IsSet(`primary-key`) {
		opts.CreateTableRequest.PrimaryKeyNames = strings.Split(strings.TrimSpace(p.options.GetString(`primary-key`)), ",")
	} else {
		primaryKey, _ := p.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: colNames,
		})
		opts.CreateTableRequest.PrimaryKeyNames = primaryKey
	}

	filePath := p.options.GetString("columns-from")
	if p.options.IsSet("columns-from") {
		columnsDefinition, err := parseJSONInputForCreateTable(filePath)
		if err != nil {
			return table.Options{}, err
		}

		opts.CreateTableRequest.Columns = columnsDefinition
	} else {
		opts.CreateTableRequest.Columns = getOptionCreateRequest(strings.Split(strings.TrimSpace(columnsStr), ","))
	}

	return opts, nil
}

func parseJSONInputForCreateTable(filePath string) ([]keboola.Column, error) {
	dataFile, err := os.ReadFile(filePath) // nolint: forbidigo
	if err != nil {
		return nil, err
	}

	var result []keboola.Column

	err = json.Unmarshal(dataFile, &result)
	if err != nil {
		return nil, err
	}
	return result, err
}

// getOptionCreateRequest returns Options.CreateTableRequest from the flags (columns, primary keys, table name). It is used if the `columns-from` flag is not specified.
func getOptionCreateRequest(columns []string) []keboola.Column {
	var c []keboola.Column
	for _, column := range columns {
		var col keboola.Column
		col.Name = column
		col.BaseType = keboola.TypeString
		col.Definition.Type = keboola.TypeString.String()
		c = append(c, col)
	}

	return c
}
