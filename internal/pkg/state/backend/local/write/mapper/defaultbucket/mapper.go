package defaultbucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

// MapBeforeLocalSave - replace default buckets in IM with placeholders.
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	config, ok := recipe.Object.(configOrRow)
	if !ok {
		return nil
	}

	if err := m.visitStorageInputTables(config, config.GetContent(), m.replaceDefaultBucketWithPlaceholder); err != nil {
		m.logger.Warnf(`Warning: %s`, err)
	}
	return nil
}

func (m *mapper) replaceDefaultBucketWithPlaceholder(
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
	inputTable.Set(`source`, fmt.Sprintf(`{{:default-bucket:%s}}.%s`, sourceConfigState.GetRelativePath(), tableName))

	return nil
}

func (m *mapper) getDefaultBucketSourceConfig(config configOrRow, tableId string) (model.ObjectState, bool, error) {
	componentId, configId, match := m.state.Components().GetDefaultBucketByTableId(tableId)
	if !match {
		return nil, false, nil
	}

	sourceConfigKey := model.ConfigKey{
		BranchId:    config.BranchKey().Id,
		ComponentId: componentId,
		Id:          configId,
	}
	sourceConfigState, found := m.state.Get(sourceConfigKey)
	if !found {
		return nil, false, errors.NewNestedError(
			errors.Errorf(`%s not found`, sourceConfigKey.Desc()),
			errors.Errorf(`referenced from %s`, config.Desc()),
			errors.Errorf(`input mapping "%s"`, tableId),
		)
	}
	return sourceConfigState, true, nil
}
