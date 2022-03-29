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

	if err := m.visitStorageInputTables(config, config.GetContent(), m.replaceDefaultBucketWithPlaceholder); err != nil {
		m.logger.Warnf(`Warning: %s`, err)
	}
	return nil
}

func (m *defaultBucketMapper) replaceDefaultBucketWithPlaceholder(
	config configOrRow,
	sourceTableId string,
	inputTable *orderedmap.OrderedMap,
) error {
	// Get source config
	sourceConfig, found, err := m.getDefaultBucketSourceConfig(config, sourceTableId)
	if err != nil {
		return err
	} else if !found {
		return nil
	}

	// Get path to the source config
	path, err := m.state.GetPath(sourceConfig)
	if err != nil {
		return err
	}

	// Parse table ID
	tableName := strings.SplitN(sourceTableId, ".", 3)[2]

	// Replace bucket with the placeholder
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, path.RelativePath(), tableName))
	return nil
}

func (m *defaultBucketMapper) getDefaultBucketSourceConfig(config configOrRow, tableId string) (model.Object, bool, error) {
	// Get components
	components, err := m.Components()
	if err != nil {
		return nil, false, err
	}

	// Parse table ID
	componentId, configId, match := components.GetDefaultBucketByTableId(tableId)
	if !match {
		return nil, false, nil
	}

	// Get source config
	sourceConfigKey := model.ConfigKey{
		BranchId:    config.BranchKey().Id,
		ComponentId: componentId,
		Id:          configId,
	}
	sourceConfig, found := m.state.Get(sourceConfigKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`%s not found`, sourceConfigKey.String()))
		errors.Append(fmt.Errorf(`  - referenced from %s`, config.String()))
		errors.Append(fmt.Errorf(`  - input mapping "%s"`, tableId))
		return nil, false, errors
	}
	return sourceConfig, true, nil
}
