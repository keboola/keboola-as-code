package template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// UpgradeContext is similar to UseContext.
// Differences:
//   - If there is already a config / row that was generated from the template, its ID will be reused.
type UpgradeContext struct {
	*UseContext
}

func NewUpgradeContext(ctx context.Context, templateRef model.TemplateRef, objectsRoot filesystem.Fs, instanceId string, targetBranch model.BranchKey, inputs InputsValues, tickets *storageapi.TicketProvider, projectState *state.State) *UpgradeContext {
	c := &UpgradeContext{
		UseContext: NewUseContext(ctx, templateRef, objectsRoot, instanceId, targetBranch, inputs, tickets),
	}

	// Register existing IDs, so they will be reused
	configs := search.ConfigsForTemplateInstance(projectState.RemoteObjects().ConfigsWithRowsFrom(targetBranch), instanceId)
	for _, config := range configs {
		// Config must exist and corresponding ID in template must be defined
		if v := config.Metadata.ConfigTemplateId(); v != nil {
			c.registerPlaceholder(v.IdInTemplate, func(_ placeholder, cb resolveCallback) { cb(config.Id) })
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
				c.registerPlaceholder(v.IdInTemplate, func(_ placeholder, cb resolveCallback) { cb(row.Id) })
			}
		}
	}

	return c
}
