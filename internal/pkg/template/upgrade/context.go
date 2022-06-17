package upgrade

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/use"
)

// Context is similar to Context.
// Differences:
//   - If there is already a config / row that was generated from the template, its ID will be reused.
type Context struct {
	*use.Context
}

func NewContext(ctx context.Context, templateRef model.TemplateRef, objectsRoot filesystem.Fs, instanceId string, targetBranch model.BranchKey, inputs template.InputsValues, tickets *storageapi.TicketProvider, projectState *state.State) *Context {
	c := &Context{
		Context: use.NewContext(ctx, templateRef, objectsRoot, instanceId, targetBranch, inputs, tickets),
	}

	// Register existing IDs, so they will be reused
	configs := search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(targetBranch), instanceId)
	iterateTmplMetadata(
		configs,
		func(config *model.Config, idInTemplate model.ConfigId, _ []model.ConfigInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(config.Id) })
		},
		func(row *model.ConfigRow, idInTemplate model.RowId, _ []model.RowInputUsage) {
			c.RegisterPlaceholder(idInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(row.Id) })
		},
	)

	return c
}
