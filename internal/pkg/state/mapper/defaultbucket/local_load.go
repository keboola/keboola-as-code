package defaultbucket

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// AfterLocalOperation - replace placeholders with default buckets in IM.
func (m *defaultBucketMapper) AfterLocalOperation(changes *model.Changes) error {
	warnings := utils.NewMultiError()
	for _, object := range changes.Loaded() {
		config, ok := object.(configOrRow)
		if !ok {
			continue
		}
		if err := m.visitStorageInputTables(config, config.GetContent(), m.replacePlaceholderWithDefaultBucket); err != nil {
			warnings.Append(err)
		}
	}

	// Log errors as warning
	if warnings.Len() > 0 {
		m.logger.Warn(utils.PrefixError(`Warning`, warnings))
	}

	// Process renamed objects
	errors := utils.NewMultiError()
	if len(changes.Renamed()) > 0 {
		if err := m.onObjectsRename(changes.Renamed(), m.state.LocalObjects()); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func (m *defaultBucketMapper) replacePlaceholderWithDefaultBucket(
	targetConfig configOrRow,
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
	branchPath, err := m.state.GetPath(targetConfig.BranchKey())
	if err != nil {
		return fmt.Errorf(`cannot get branch path: %w`, err)
	}

	// Get key by path
	path := filesystem.Join(branchPath.String(), splitSource[0])
	sourceConfigRaw, found := m.state.GetByPath(path)
	if !found {
		return fmt.Errorf(
			`%s contains table "%s" in input mapping referencing to a non-existing configuration`,
			targetConfig.String(),
			inputTableSource,
		)
	}
	sourceConfig := sourceConfigRaw.(*model.Config)

	defaultBucket, found := m.Components().GetDefaultBucketByComponentId(sourceConfig.ComponentId, sourceConfig.Id)
	if !found {
		return nil
	}

	inputTable.Set(`source`, fmt.Sprintf("%s.%s", defaultBucket, splitSource[1]))
	markUsedInInputMapping(sourceConfig, targetConfig)
	return nil
}
