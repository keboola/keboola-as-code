package upgrade

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type (
	configFn func(config *model.Config, idInTemplate model.ConfigId, inputs []model.ConfigInputUsage)
	rowFn    func(row *model.ConfigRow, idInTemplate model.RowId, inputs []model.RowInputUsage)
)

func iterateTmplMetadata(configs []*model.ConfigWithRows, c configFn, r rowFn) {
	for _, config := range configs {
		// Config must exist and corresponding ID in template must be defined
		if v := config.Metadata.ConfigTemplateId(); v != nil {
			c(config.Config, v.IdInTemplate, config.Metadata.InputsUsage())
		} else {
			continue
		}

		// Convert slices to maps
		rowsIdsMap := make(map[model.RowId]model.RowIdMetadata)
		for _, v := range config.Metadata.RowsTemplateIds() {
			rowsIdsMap[v.IdInProject] = v
		}
		rowsInputsMap := make(map[model.RowId][]model.RowInputUsage)
		for _, v := range config.Metadata.RowsInputsUsage() {
			rowsInputsMap[v.RowId] = append(rowsInputsMap[v.RowId], v)
		}

		// Process existing rows
		for _, row := range config.Rows {
			// Row must exist and corresponding ID in template must be defined
			if v, found := rowsIdsMap[row.Id]; found {
				r(row, v.IdInTemplate, rowsInputsMap[row.Id])
			}
		}
	}
}
