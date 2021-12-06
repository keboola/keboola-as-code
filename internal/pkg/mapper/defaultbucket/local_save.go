package defaultbucket

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// MapBeforeLocalSave - replace default buckets in IM with placeholders.
func (m *defaultBucketMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	inputTablesRaw := utils.GetFromMap(config.Content, []string{"storage", "input", "tables"})
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

		m.replaceDefaultBucketWithPlaceholder(config, inputTableSource, inputTable)
	}

	return nil
}

func (m *defaultBucketMapper) replaceDefaultBucketWithPlaceholder(config *model.Config, sourceTableId string, inputTable *orderedmap.OrderedMap) {
	sourceConfigPath, found := m.getDefaultBucketSourceConfigurationPath(config, sourceTableId)
	if !found {
		return
	}

	tableName := strings.Split(sourceTableId, ".")[2]
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, sourceConfigPath, tableName))
}

func (m *defaultBucketMapper) getDefaultBucketSourceConfigurationPath(config *model.Config, tableId string) (string, bool) {
	componentId, configId, match := m.State.Components().MatchDefaultBucketInTableId(tableId)
	if !match {
		return "", false
	}

	sourceConfigKey := model.ConfigKey{
		BranchId:    config.BranchId,
		ComponentId: componentId,
		Id:          configId,
	}
	sourceConfig, found := m.State.Get(sourceConfigKey)
	if !found {
		m.Logger.Warnf(`Warning: configuration "%s" of component "%s" that was supposed to create table "%s" in the input mapping of configuration "%s" not found`, configId, componentId, tableId, config.Id)
		return "", false
	}
	return sourceConfig.GetObjectPath(), true
}
