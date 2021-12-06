package defaultbucket

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type defaultBucketMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *defaultBucketMapper {
	return &defaultBucketMapper{MapperContext: context}
}

func (m *defaultBucketMapper) visitStorageInputTables(config *model.Config, callback func(config *model.Config, inputTableSource string, inputTable *orderedmap.OrderedMap) error) error {
	inputTablesRaw, found, err := config.Content.GetNested("storage.input.tables")
	if !found {
		return nil
	}
	if err != nil {
		return err
	}
	inputTables, ok := inputTablesRaw.([]interface{})
	if !ok {
		return nil
	}

	for _, inputTableRaw := range inputTables {
		inputTable, ok := inputTableRaw.(*orderedmap.OrderedMap)
		if !ok {
			continue
		}
		inputTableSourceRaw, ok := inputTable.Get(`source`)
		if !ok {
			continue
		}
		inputTableSource, ok := inputTableSourceRaw.(string)
		if !ok {
			continue
		}

		err := callback(config, inputTableSource, inputTable)
		if err != nil {
			return err
		}
	}

	return nil
}
