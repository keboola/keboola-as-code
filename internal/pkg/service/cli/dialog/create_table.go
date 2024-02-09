package dialog

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

const (
	columnsNamesFlag columnsDefinitionMethod = iota
	columnsNamesInteractive
	columnsDefinitionFlag
	columnsDefinitionInteractive
)

type columnsDefinitionMethod int

func (p *Dialogs) AskCreateTable(args []string, branchKey keboola.BranchKey, allBuckets []*keboola.Bucket) (table.Options, error) {
	opts := table.Options{}

	// Table ID and name
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
				Validator:   prompt.ValueRequired,
			})
		}
		opts.CreateTableRequest.Name = name
	}

	// Columns
	columnsMethod, err := p.columnsDefinitionMethod()
	if err != nil {
		return opts, err
	}

	switch columnsMethod {
	case columnsNamesFlag:
		columnsStr := p.options.GetString(`columns`)
		colNames := strings.Split(strings.TrimSpace(columnsStr), ",")
		opts.CreateTableRequest.Columns = getOptionCreateRequest(colNames)
	case columnsDefinitionFlag:
		filePath := p.options.GetString("columns-from")
		columnsDefinition, err := parseJSONInputForCreateTable(filePath)
		if err != nil {
			return table.Options{}, err
		}
		opts.CreateTableRequest.Columns = columnsDefinition
	case columnsNamesInteractive:
		columnsStr := p.options.GetString(`columns`)
		if !p.options.IsSet(`columns`) {
			columnsStr, _ = p.Ask(&prompt.Question{
				Label:       "Columns",
				Description: "Enter a comma-separated list of column names.",
			})
		}
		colNames := strings.Split(strings.TrimSpace(columnsStr), ",")
		opts.CreateTableRequest.Columns = getOptionCreateRequest(colNames)
	case columnsDefinitionInteractive:
		input, _ := p.Editor("yaml", &prompt.Question{
			Label:       "Columns definitions",
			Description: "Columns definitions",
			Default:     p.defaultValue(),
			Validator: func(val any) error {
				_, err := parseColumnsDefinitionFromFile(val.(string))
				if err != nil {
					return err
				}
				return nil
			},
		})
		res, err := parseColumnsDefinitionFromFile(input)
		if err != nil {
			return table.Options{}, err
		}
		opts.CreateTableRequest.Columns = res
	default:
		panic(errors.New("unexpected state"))
	}

	// Primary keys
	if p.options.IsSet(`primary-key`) {
		opts.CreateTableRequest.PrimaryKeyNames = strings.Split(strings.TrimSpace(p.options.GetString(`primary-key`)), ",")
	} else {
		primaryKey, _ := p.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: getColumnsName(opts.CreateTableRequest.Columns),
		})
		opts.CreateTableRequest.PrimaryKeyNames = primaryKey
	}

	return opts, nil
}

func (p *Dialogs) columnsDefinitionMethod() (columnsDefinitionMethod, error) {
	switch {
	case !p.options.IsSet("columns-from") && !p.options.IsSet("columns"):
		// Ask for method
		specifyTypes := p.Prompt.Confirm(&prompt.Confirm{
			Label:       "Column types",
			Description: "Want to define column types? Otherwise all columns default to strings.",
			Default:     true,
		})
		if specifyTypes {
			return columnsDefinitionInteractive, nil
		} else {
			return columnsNamesInteractive, nil
		}
	case p.options.IsSet("columns-from") && p.options.IsSet("columns"):
		// Only one flag can be specified at the same time
		return 0, errors.New("can't be specified both flag together, use only one of them")
	case p.options.IsSet("columns"):
		return columnsNamesFlag, nil
	case p.options.IsSet("columns-from"):
		return columnsDefinitionFlag, nil
	default:
		panic(errors.New("unexpected state"))
	}
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

func parseColumnsDefinitionFromFile(input string) ([]keboola.Column, error) {
	var result []keboola.Column

	err := yaml.Unmarshal([]byte(input), &result)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (p *Dialogs) defaultValue() string {
	fileHeader := `#Command "remote create table"

#Edit or replace this part of the text with your definition. Keep the same format.Then save your changes and close the editor:

- name: id
  definition:
    type: VARCHAR
  basetype: STRING
- name: name
  definition:
    type: VARCHAR
  basetype: STRING
`
	return fileHeader
}

func getColumnsName(columns []keboola.Column) []string {
	var result []string
	for _, column := range columns {
		result = append(result, column.Name)
	}
	return result
}
