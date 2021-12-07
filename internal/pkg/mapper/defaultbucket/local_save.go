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

	err := m.visitStorageInputTables(config, m.replaceDefaultBucketWithPlaceholder)
	if err != nil {
		m.Logger.Warnf(`Warning: %s`, err)
	}
	return nil
}

func (m *defaultBucketMapper) replaceDefaultBucketWithPlaceholder(config *model.Config, sourceTableId string, inputTable *orderedmap.OrderedMap) error {
	sourceConfigPath, found, err := m.getDefaultBucketSourceConfigurationPath(config, sourceTableId)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	tableName := strings.SplitN(sourceTableId, ".", 3)[2]
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, sourceConfigPath, tableName))

	return nil
}

func (m *defaultBucketMapper) getDefaultBucketSourceConfigurationPath(config *model.Config, tableId string) (string, bool, error) {
	componentId, configId, match := m.State.Components().GetDefaultBucketByTableId(tableId)
	if !match {
		return "", false, nil
	}

	sourceConfigKey := model.ConfigKey{
		BranchId:    config.BranchId,
		ComponentId: componentId,
		Id:          configId,
	}
	sourceConfig, found := m.State.Get(sourceConfigKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`%s not found`, sourceConfigKey.Desc()))
		errors.Append(fmt.Errorf(`  - referenced from %s`, config.Desc()))
		errors.Append(fmt.Errorf(`  - input mapping "%s"`, tableId))
		return "", false, errors
	}
	return sourceConfig.GetObjectPath(), true, nil
}
