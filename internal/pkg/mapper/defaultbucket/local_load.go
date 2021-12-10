package defaultbucket

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// OnLocalChange - replace placeholders with default buckets in IM.
func (m *defaultBucketMapper) OnLocalChange(changes *model.LocalChanges) error {
	errors := utils.NewMultiError()
	for _, objectState := range changes.Loaded() {
		config, ok := objectState.LocalState().(configOrRow)
		if !ok {
			continue
		}
		if err := m.visitStorageInputTables(config, config.GetContent(), m.replacePlaceholderWithDefaultBucket); err != nil {
			errors.Append(err)
		}
	}

	// Log errors as warning
	if errors.Len() > 0 {
		m.Logger.Warn(utils.PrefixError(`Warning`, errors))
	}

	return nil
}

func (m *defaultBucketMapper) replacePlaceholderWithDefaultBucket(
	config configOrRow,
	inputTableSource string,
	inputTable *orderedmap.OrderedMap,
) error {
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
		return fmt.Errorf(
			`%s contains table "%s" in input mapping referencing to a non-existing configuration`,
			config.Desc(),
			inputTableSource,
		)
	}
	configKey, ok := configKeyRaw.(model.ConfigKey)
	if !ok {
		return nil
	}

	defaultBucket, found := m.State.Components().GetDefaultBucketByComponentId(configKey.ComponentId, configKey.Id)
	if !found {
		return nil
	}

	inputTable.Set(`source`, fmt.Sprintf("%s.%s", defaultBucket, splitSource[1]))

	return nil
}
