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
	config, ok := recipe.Object.(configOrRow)
	if !ok {
		return nil
	}

	configFile, err := recipe.Files.ObjectConfigFile()
	if err != nil {
		panic(err)
	}

	if err := m.visitStorageInputTables(config, configFile.Content, m.replaceDefaultBucketWithPlaceholder); err != nil {
		m.Logger.Warnf(`Warning: %s`, err)
	}
	return nil
}

func (m *defaultBucketMapper) replaceDefaultBucketWithPlaceholder(
	config configOrRow,
	sourceTableId string,
	inputTable *orderedmap.OrderedMap,
) error {
	sourceConfigState, found, err := m.getDefaultBucketSourceConfig(config, sourceTableId)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	tableName := strings.SplitN(sourceTableId, ".", 3)[2]
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, sourceConfigState.GetObjectPath(), tableName))

	return nil
}

func (m *defaultBucketMapper) getDefaultBucketSourceConfig(config configOrRow, tableId string) (model.ObjectState, bool, error) {
	componentId, configId, match := m.State.Components().GetDefaultBucketByTableId(tableId)
	if !match {
		return nil, false, nil
	}

	sourceConfigKey := model.ConfigKey{
		BranchId:    config.BranchKey().Id,
		ComponentId: componentId,
		Id:          configId,
	}
	sourceConfigState, found := m.State.Get(sourceConfigKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`%s not found`, sourceConfigKey.Desc()))
		errors.Append(fmt.Errorf(`  - referenced from %s`, config.Desc()))
		errors.Append(fmt.Errorf(`  - input mapping "%s"`, tableId))
		return nil, false, errors
	}
	return sourceConfigState, true, nil
}
