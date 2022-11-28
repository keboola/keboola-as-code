// Package upgrade represents the process of replacing values when upgrading a template instance.
package upgrade

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/use"
)

// Context represents the process of the replacing values when upgrading a template instance.
// It is similar and extends the use.Context.
// Differences:
//   - If there is already a config / row that was generated from the template, its ID will be reused.
type Context struct {
	*use.Context
}

func NewContext(ctx context.Context, templateRef model.TemplateRef, objectsRoot filesystem.Fs, instanceID string, targetBranch model.BranchKey, inputsValues template.InputsValues, inputsDefs map[string]*template.Input, tickets *storageapi.TicketProvider, components *model.ComponentsMap, projectState *state.State) *Context {
	c := &Context{
		Context: use.NewContext(ctx, templateRef, objectsRoot, instanceID, targetBranch, inputsValues, inputsDefs, tickets, components),
	}

	// Register existing IDs, so they will be reused
	configs := search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(targetBranch), instanceID)
	iterateTmplMetadata(
		configs,
		func(config *model.Config, idInTemplate storageapi.ConfigID, _ []model.ConfigInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(config.ID) })
		},
		func(row *model.ConfigRow, idInTemplate storageapi.RowID, _ []model.RowInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(row.ID) })
		},
	)

	return c
}
