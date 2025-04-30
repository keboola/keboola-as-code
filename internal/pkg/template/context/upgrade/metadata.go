package upgrade

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type (
	configFn func(config *model.Config, idInTemplate keboola.ConfigID, inputs []model.ConfigInputUsage)
	rowFn    func(row *model.ConfigRow, idInTemplate keboola.RowID, inputs []model.RowInputUsage)
)

func iterateTmplMetadata(configs []*model.ConfigWithRows, c configFn, r rowFn) {
	for _, config := range configs {
		// Always include config rows from a shared code config
		if config.ComponentID != keboola.SharedCodeComponentID {
			// Config must exist and corresponding ID in template must be defined
			if v := config.Metadata.ConfigTemplateID(); v != nil {
				c(config.Config, v.IDInTemplate, config.Metadata.InputsUsage())
			} else {
				continue
			}
		}

		// Convert slices to maps
		rowsIdsMap := make(map[keboola.RowID]model.RowIDMetadata)
		for _, v := range config.Metadata.RowsTemplateIds() {
			rowsIdsMap[v.IDInProject] = v
		}
		rowsInputsMap := make(map[keboola.RowID][]model.RowInputUsage)
		for _, v := range config.Metadata.RowsInputsUsage() {
			rowsInputsMap[v.RowID] = append(rowsInputsMap[v.RowID], v)
		}

		// Process existing rows
		for _, row := range config.Rows {
			// Row must exist and corresponding ID in template must be defined
			if v, found := rowsIdsMap[row.ID]; found {
				r(row, v.IDInTemplate, rowsInputsMap[row.ID])
			}
		}
	}
}
