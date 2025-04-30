package create

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

const (
	BackendBigQuery                          = "bigquery"
	columnsNamesFlag columnsDefinitionMethod = iota
	columnsNamesInteractive
	columnsDefinitionFlag
	columnsDefinitionInteractive
)

type columnsDefinitionMethod int

func AskCreateTable(args []string, branchKey keboola.BranchKey, allBuckets []*keboola.Bucket, d *dialog.Dialogs, f Flags, backends []string) (table.Options, error) {
	opts := table.Options{}

	if f.OptionsFrom.Value != "" {
		if !slices.Contains(backends, BackendBigQuery) {
			return opts, errors.Errorf(`project backend have to be "%s" to use --options-from flag`, BackendBigQuery)
		}

		if f.ColumnsFrom.Value == "" {
			return opts, errors.Errorf("columns-from must be set for bigquery settings")
		}

		bigQueryOptions, err := parseOptionsFromFile(f.OptionsFrom.Value)
		if err != nil {
			return opts, err
		}
		opts.CreateTableRequest.TableDefinition = bigQueryOptions
	}

	if len(args) == 1 {
		tableID, err := keboola.ParseTableID(args[0])
		if err != nil {
			return opts, err
		}
		opts.BucketKey = keboola.BucketKey{BranchID: branchKey.ID, BucketID: tableID.BucketID}
		opts.CreateTableRequest.Name = tableID.TableName
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
		opts.CreateTableRequest.Name = name
	}

	// Columns
	columnsMethod, err := columnsDefinition(d, f)
	if err != nil {
		return opts, err
	}

	switch columnsMethod {
	case columnsNamesFlag:
		opts.CreateTableRequest.Columns = getOptionCreateRequest(f.Columns.Value)
	case columnsDefinitionFlag:
		filePath := f.ColumnsFrom.Value
		definition, err := ParseJSONInputForCreateTable(filePath)
		if err != nil {
			return table.Options{}, err
		}
		opts.CreateTableRequest.Columns = definition
	case columnsNamesInteractive:
		if !f.Columns.IsSet() {
			columnsStr, _ := d.Ask(&prompt.Question{
				Label:       "Columns",
				Description: "Enter a comma-separated list of column names.",
			})
			f.Columns.Value = strings.Split(strings.TrimSpace(columnsStr), ",")
		}
		opts.CreateTableRequest.Columns = getOptionCreateRequest(f.Columns.Value)
	case columnsDefinitionInteractive:
		input, _ := d.Editor("yaml", &prompt.Question{
			Label:       "Columns definitions",
			Description: "Columns definitions",
			Default:     defaultValue(),
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

	if f.PrimaryKey.Value != "" {
		opts.CreateTableRequest.PrimaryKeyNames = strings.Split(strings.TrimSpace(f.PrimaryKey.Value), ",")
	} else if !f.PrimaryKey.IsSet() {
		primaryKeys, _ := d.MultiSelect(&prompt.MultiSelect{
			Label:   "Select columns for primary key",
			Options: possiblePrimaryKeys(opts.CreateTableRequest.Columns),
		})

		opts.CreateTableRequest.PrimaryKeyNames = primaryKeys
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

func columnsDefinition(d *dialog.Dialogs, f Flags) (columnsDefinitionMethod, error) {
	switch {
	case !f.ColumnsFrom.IsSet() && !f.Columns.IsSet():
		// Ask for method
		specifyTypes := d.Confirm(&prompt.Confirm{
			Label:       "Column types",
			Description: "Want to define column types? Otherwise all columns default to strings.",
			Default:     true,
		})
		if specifyTypes {
			return columnsDefinitionInteractive, nil
		} else {
			return columnsNamesInteractive, nil
		}
	case f.ColumnsFrom.IsSet() && f.Columns.IsSet():
		// Only one flag can be specified at the same time
		return 0, errors.New("can't be specified both flag together, use only one of them")
	case f.Columns.IsSet():
		return columnsNamesFlag, nil
	case f.ColumnsFrom.IsSet():
		return columnsDefinitionFlag, nil
	default:
		panic(errors.New("unexpected state"))
	}
}

func ParseJSONInputForCreateTable(filePath string) ([]keboola.Column, error) {
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
	c := make([]keboola.Column, 0, len(columns))
	for _, column := range columns {
		var col keboola.Column
		col.Name = column
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

func parseOptionsFromFile(filePath string) (keboola.TableDefinition, error) {
	dataFile, err := os.ReadFile(filePath) // nolint: forbidigo
	if err != nil {
		return keboola.TableDefinition{}, err
	}

	o := keboola.TableDefinition{}

	err = json.Unmarshal(dataFile, &o)
	if err != nil {
		return keboola.TableDefinition{}, err
	}

	return o, nil
}

func defaultValue() string {
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

func possiblePrimaryKeys(columns []keboola.Column) []string {
	var result []string
	for _, column := range columns {
		if column.Definition == nil || !column.Definition.Nullable {
			result = append(result, column.Name)
		}
	}

	return result
}
