// Package create represents the process of replacing values when creating a template from a remote project.
package create

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

// Context represents the process of replacing values when creating a template from a remote project.
//
// Process description:
//  1. There is some remote project.
//     - It has unique objects IDs. For example ID "12345" for a config.
//  2. When creating a template, the user defines a human-readable ID for each object.
//     - For example "my-config-id" for "12345" config.
//     - See "dialog.AskCreateTemplateOpts".
//     - Not all project objects need to be copied to the template, see Context.RemoteObjectsFilter.
//  3. Project is pulled to the template, see "pkg/lib/operation/template/sync/pull"
//     - Each ID is replaced by a function placeholder.
//     For example "12345" -> "<<~~func:ConfigId:["my-config-id"]~~>>".
//     - This is because we use Json as an intermediate step in generating a Jsonnet template.
//     - In Json we can't define a function. Therefore, we use the string placeholder.
//     - IDs -> placeholders are replaced by "internal/pkg/mapper/template/replacevalues"
//  4. Template is saved to the filesystem.
//     - Json files are converted to Jsonnet by "internal/pkg/mapper/template/jsonnetfiles"
//     - Placeholders are replaced by function calls.
//     - For example "foo <<~~func:ConfigId:["my-config-id"]~~>> bar" is replaced by "foo " + ConfigId("my-config-id") + " bar".
//     - Functions calls are generated by "jsonnet.FormatAst", and "jsonnet.ReplacePlaceholdersRecursive".
//
// Context.RemoteObjectsFilter() defines which objects will be part of the template.
// Context.Replacements() returns placeholders for ConfigId / ConfigRowId Jsonnet functions.
type Context struct {
	_context
	remoteFilter model.ObjectsFilter
	replacements *replacevalues.Values
}

type _context context.Context

type InputDef struct {
	Path    orderedmap.Path
	InputID string
}

type ConfigDef struct {
	Key        model.ConfigKey
	TemplateID string
	Inputs     []InputDef
	Rows       []ConfigRowDef
}

type ConfigRowDef struct {
	Key        model.ConfigRowKey
	TemplateID string
	Inputs     []InputDef
}

func NewContext(ctx context.Context, sourceBranch model.BranchKey, configs []ConfigDef) *Context {
	return &Context{
		_context:     template.NewContext(ctx),
		remoteFilter: remoteFilterForCreate(sourceBranch, configs),
		replacements: replacementsForCreate(sourceBranch, configs),
	}
}

func (c *Context) RemoteObjectsFilter() model.ObjectsFilter {
	return c.remoteFilter
}

func (c *Context) LocalObjectsFilter() model.ObjectsFilter {
	return model.NoFilter()
}

func (c *Context) JsonnetContext() *jsonnet.Context {
	// When saving a template, nothing needs to be set.
	return nil
}

func (c *Context) Replacements() (*replacevalues.Values, error) {
	return c.replacements, nil
}

func replacementsForCreate(sourceBranch model.BranchKey, configs []ConfigDef) *replacevalues.Values {
	replacements := replacevalues.NewValues()

	// Replace BranchID, in template all objects have BranchID = 0
	replacements.AddKey(sourceBranch, model.BranchKey{ID: 0})

	// Configs
	for _, config := range configs {
		newConfigID := keboola.ConfigID(jsonnet.ConfigIDPlaceholder(config.TemplateID))
		newConfigKey := config.Key
		newConfigKey.BranchID = 0
		newConfigKey.ID = newConfigID
		replacements.AddKey(config.Key, newConfigKey)

		// Config inputs
		for _, input := range config.Inputs {
			replacements.AddContentField(config.Key, input.Path, jsonnet.InputPlaceholder(input.InputID))
		}

		// Rows
		for _, row := range config.Rows {
			newRowID := keboola.RowID(jsonnet.ConfigRowIDPlaceholder(row.TemplateID))
			newRowKey := row.Key
			newRowKey.BranchID = 0
			newRowKey.ConfigID = newConfigID
			newRowKey.ID = newRowID
			replacements.AddKey(row.Key, newRowKey)

			// Row inputs
			for _, input := range row.Inputs {
				replacements.AddContentField(row.Key, input.Path, jsonnet.InputPlaceholder(input.InputID))
			}
		}
	}

	return replacements
}

func remoteFilterForCreate(sourceBranch model.BranchKey, configs []ConfigDef) model.ObjectsFilter {
	keys := make([]model.Key, 0, len(configs)+1)
	// Branch
	keys = append(keys, sourceBranch)

	// Configs and rows
	for _, config := range configs {
		keys = append(keys, config.Key)
		for _, row := range config.Rows {
			keys = append(keys, row.Key)
		}
	}

	filter := model.NoFilter()
	filter.SetAllowedKeys(keys)
	return filter
}
