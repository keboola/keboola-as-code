package defaultbucket

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// MapAfterLocalLoad - replace placeholders with default buckets in IM.
func (m *defaultBucketMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	err := m.visitStorageInputTables(config, m.replacePlaceholderWithDefaultBucket)
	if err != nil {
		m.Logger.Warnf(`Warning: %s`, err)
	}
	return nil
}

func (m *defaultBucketMapper) replacePlaceholderWithDefaultBucket(config *model.Config, inputTableSource string, inputTable *orderedmap.OrderedMap) error {
	if !strings.HasPrefix(inputTableSource, "{{:default-bucket:") {
		return nil
	}

	sourceWithoutPrefix := strings.TrimPrefix(inputTableSource, "{{:default-bucket:")
	splitSource := strings.Split(sourceWithoutPrefix, "}}.")
	if len(splitSource) != 2 {
		return nil
	}

	// Get branch
	branch := m.State.MustGet(config.BranchKey())

	// Get key by path
	path := filesystem.Join(branch.Path(), splitSource[0])
	configKeyRaw, found := m.Naming.FindByPath(path)
	if !found {
		return fmt.Errorf(`configuration "%s" contains table "%s" in input mapping referencing to a non-existing configuration`, config.Id, inputTableSource)
	}
	configKey, ok := configKeyRaw.(model.ConfigKey)
	if !ok {
		return nil
	}

	defaultBucket, found := m.State.Components().GetDefaultBucket(configKey.ComponentId, configKey.Id)
	if !found {
		return nil
	}

	inputTable.Set(`source`, fmt.Sprintf("%s.%s", defaultBucket, splitSource[1]))

	return nil
}
