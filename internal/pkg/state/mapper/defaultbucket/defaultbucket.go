package defaultbucket

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type defaultBucketMapper struct {
	dependencies
	logger log.Logger
}

type configOrRow interface {
	model.ObjectWithContent
	GetBranchKey() model.BranchKey
}

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

func NewLocalMapper(d dependencies) *defaultBucketMapper {
	return &defaultBucketMapper{dependencies: d, logger: d.Logger()}
}

type callbackFn func(state *local.State, config configOrRow, sourceTableId string, storageInputTable *orderedmap.OrderedMap) error

func (m *defaultBucketMapper) visitStorageInputTables(state *local.State, config configOrRow, content *orderedmap.OrderedMap, callback callbackFn) error {
	inputTablesRaw, _, _ := content.GetNested("storage.input.tables")
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

		err := callback(state, config, inputTableSource, inputTable)
		if err != nil {
			return err
		}
	}

	return nil
}

func markUsedInInputMapping(omConfig *model.Config, usedIn configOrRow) {
	switch v := usedIn.(type) {
	case *model.Config:
		omConfig.Relations.Add(&model.UsedInConfigInputMappingRelation{
			UsedIn: v.ConfigKey,
		})
	case *model.ConfigRow:
		omConfig.Relations.Add(&model.UsedInRowInputMappingRelation{
			UsedIn: v.ConfigRowKey,
		})
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, usedIn))
	}
}
