package upgrade

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type (
	configFn func(config *model.Config, idInTemplate storageapi.ConfigID, inputs []model.ConfigInputUsage)
	rowFn    func(row *model.ConfigRow, idInTemplate storageapi.RowID, inputs []model.RowInputUsage)
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
		rowsIdsMap := make(map[storageapi.RowID]model.RowIdMetadata)
		for _, v := range config.Metadata.RowsTemplateIds() {
			rowsIdsMap[v.IdInProject] = v
		}
		rowsInputsMap := make(map[storageapi.RowID][]model.RowInputUsage)
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
