package defaultbucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeLocalSave - replace default buckets in IM with placeholders.
func (m *defaultBucketMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	config, ok := recipe.Object.(configOrRow)
	if !ok {
		return nil
	}

	if err := m.visitStorageInputTables(config, config.GetContent(), m.replaceDefaultBucketWithPlaceholder); err != nil {
		m.logger.Warnf(ctx, `Warning: %s`, err)
	}
	return nil
}

func (m *defaultBucketMapper) replaceDefaultBucketWithPlaceholder(
	config configOrRow,
	sourceTableID string,
	inputTable *orderedmap.OrderedMap,
) error {
	sourceConfigState, found, err := m.getDefaultBucketSourceConfig(config, sourceTableID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	tableName := strings.SplitN(sourceTableID, ".", 3)[2]
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, sourceConfigState.GetRelativePath(), tableName))

	return nil
}

func (m *defaultBucketMapper) getDefaultBucketSourceConfig(config configOrRow, tableID string) (model.ObjectState, bool, error) {
	componentID, configID, match := m.state.Components().GetDefaultBucketByTableID(tableID)
	if !match {
		return nil, false, nil
	}

	sourceConfigKey := model.ConfigKey{
		BranchID:    config.BranchKey().ID,
		ComponentID: componentID,
		ID:          configID,
	}
	sourceConfigState, found := m.state.Get(sourceConfigKey)
	if !found {
		return nil, false, errors.NewNestedError(
			errors.Errorf(`%s not found`, sourceConfigKey.Desc()),
			errors.Errorf(`referenced from %s`, config.Desc()),
			errors.Errorf(`input mapping "%s"`, tableID),
		)
	}
	return sourceConfigState, true, nil
}
