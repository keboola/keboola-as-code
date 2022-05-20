package upgrade

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
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
	configs := search.ConfigsForTemplateInstance(projectState.RemoteObjects().ConfigsWithRowsFrom(targetBranch), instanceId)
	for _, config := range configs {
		// Config must exist and corresponding ID in template must be defined
		if v := config.Metadata.ConfigTemplateId(); v != nil {
			c.RegisterPlaceholder(v.IdInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(config.Id) })
		} else {
			continue
		}

		// Convert slice to map
		rowsIdsMap := make(map[model.RowId]model.RowIdMetadata)
		for _, v := range config.Metadata.RowsTemplateIds() {
			rowsIdsMap[v.IdInProject] = v
		}

		// Process existing rows
		for _, row := range config.Rows {
			// Row must exist and corresponding ID in template must be defined
			if v, found := rowsIdsMap[row.Id]; found {
				c.RegisterPlaceholder(v.IdInTemplate, func(_ use.Placeholder, cb use.ResolveCallback) { cb(row.Id) })
			}
		}
	}

	return c
}
