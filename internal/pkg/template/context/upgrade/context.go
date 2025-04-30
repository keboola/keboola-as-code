// Package upgrade represents the process of replacing values when upgrading a template instance.
package upgrade

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/use"
)

// dataAppInstanceIDContentPath contains path to the app instance ID in the configuration content.
// There is no ID when creating the configuration.
// The ID is set by the application deploy job after the application deployment.
// The ID must be preserved when the template instance is upgraded.
const dataAppInstanceIDContentPath = "parameters.id"

// Context represents the process of the replacing values when upgrading a template instance.
// It is similar and extends the use.Context.
// Differences:
//   - If there is already a config / row that was generated from the template, its ID will be reused.
type Context struct {
	*use.Context
}

func NewContext(ctx context.Context, templateRef model.TemplateRef, objectsRoot filesystem.Fs, instanceID string, targetBranch model.BranchKey, inputsValues template.InputsValues, inputsDefs map[string]*template.Input, tickets *keboola.TicketProvider, components *model.ComponentsMap, projectState *state.State, backends []string) *Context {
	c := &Context{
		Context: use.NewContext(ctx, templateRef, objectsRoot, instanceID, targetBranch, inputsValues, inputsDefs, tickets, components, projectState, backends),
	}

	// Register existing IDs, so they will be reused
	configs := search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(targetBranch), instanceID)
	iterateTmplMetadata(
		configs,
		func(config *model.Config, idInTemplate keboola.ConfigID, _ []model.ConfigInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(config.ID) })

			// Preserve application instance ID when the template instance is upgraded.
			if config.ComponentID == keboola.DataAppsComponentID {
				appInstanceID, found1, _ := config.Content.GetNested(dataAppInstanceIDContentPath)
				placeholder, found2 := c.Placeholders()[idInTemplate]
				if found1 && found2 {
					// Jsonnet template is evaluated to a temporary JSON with unique placeholders.
					// Later, the placeholders in the JSON are replaced.
					// This is because we do not know in advance how many new IDs we will need to generate.
					// And generating them one by one would take a long time.
					// We need to identify the application config in this temporary form.
					key := config.ConfigKey
					key.BranchID = 0
					key.ID = placeholder.Value().(keboola.ConfigID)
					c.ReplaceContentField(key, orderedmap.PathFromStr(dataAppInstanceIDContentPath), appInstanceID)
				}
			}
		},
		func(row *model.ConfigRow, idInTemplate keboola.RowID, _ []model.RowInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(row.ID) })
		},
	)

	return c
}
