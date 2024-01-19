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

	filePath := p.options.GetString("columns-from")
	if p.options.IsSet("columns-from") && len(opts.PrimaryKey) == 0 && opts.Name == "" {
		createTableRequest, err := parseJSONInputForCreateTable(filePath)
		if err != nil {
			return table.Options{}, err
		}
		opts.CreateTableRequest = *createTableRequest
	} else {
		opts = getOptionCreateRequest(opts)
	}

	return opts, nil
}

func parseJSONInputForCreateTable(filePath string) (*keboola.CreateTableRequest, error) {
	dataFile, err := os.ReadFile(filePath) // nolint: forbidigo
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

// this function returns the created CreateTableRequest from the flag from the statement (columns, primary keys, table name), if command don't include flag 'columns-from'.
func getOptionCreateRequest(opts table.Options) table.Options {
	var columns []keboola.Column
	for _, column := range opts.Columns {
		var c keboola.Column
		c.Name = column
		c.BaseType = keboola.TypeString
		c.Definition.Type = keboola.TypeString.String()
		columns = append(columns, c)
	}

	createTableRequest := keboola.CreateTableRequest{
		TableDefinition: keboola.TableDefinition{
			PrimaryKeyNames: opts.PrimaryKey,
			Columns:         columns,
		},
		Name: opts.Name,
	}

	opts.CreateTableRequest = createTableRequest

	return opts
}
