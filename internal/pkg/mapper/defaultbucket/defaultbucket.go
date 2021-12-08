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

func (m *defaultBucketMapper) visitStorageInputTables(
	branchKey model.BranchKey,
	configDesc string,
	configContent *orderedmap.OrderedMap,
	callback func(
		branchKey model.BranchKey,
		configDesc string,
		sourceTableId string,
		storageInputTable *orderedmap.OrderedMap,
	) error,
) error {
	inputTablesRaw, _, _ := configContent.GetNested("storage.input.tables")
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

		err := callback(branchKey, configDesc, inputTableSource, inputTable)
		if err != nil {
			return err
		}
	}

	return nil
}
