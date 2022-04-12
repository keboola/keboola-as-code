package defaultbucket

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// AfterLocalOperation - replace placeholders with default buckets in IM.
func (m *defaultBucketMapper) AfterLocalOperation(state *local.State, changes *model.Changes) error {
	warnings := errors.NewMultiError()
	for _, object := range changes.Loaded() {
		config, ok := object.(configOrRow)
		if !ok {
			continue
		}
		if err := m.visitStorageInputTables(state, config, config.GetContent(), m.replacePlaceholder); err != nil {
			warnings.Append(err)
		}
	}

	// Log errors as warning
	if warnings.Len() > 0 {
		m.logger.Warn(errors.PrefixError(`Warning`, warnings))
	}

	return nil
}

func (m *defaultBucketMapper) replacePlaceholder(
	state *local.State,
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

	// Get branch path
	branchPath, err := state.GetPath(targetConfig.GetBranchKey())
	if err != nil {
		return err
	}

	// Get key by path
	sourcePath := filesystem.Join(branchPath.String(), splitSource[0])
	sourceConfigRaw, found := state.GetByPath(sourcePath)
	if !found {
		return fmt.Errorf(
			`%s contains table "%s" in input mapping referencing to a non-existing configuration`,
			targetConfig.String(),
			inputTableSource,
		)
	}
	sourceConfig := sourceConfigRaw.(*model.Config)

	// Get components
	components, err := m.Components()
	if err != nil {
		return err
	}

	// Get default bucket
	defaultBucket, found := components.GetDefaultBucketByComponentId(sourceConfig.ComponentId, sourceConfig.ConfigId)
	if !found {
		return nil
	}

	// Replace placeholder with the bucket
	inputTable.Set(`source`, fmt.Sprintf("%s.%s", defaultBucket, splitSource[1]))
	markUsedInInputMapping(sourceConfig, targetConfig)
	return nil
}
