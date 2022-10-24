package defaultbucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AfterLocalOperation - replace placeholders with default buckets in IM.
func (m *defaultBucketMapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
	warnings := errors.NewMultiError()
	for _, objectState := range changes.Loaded() {
		config, ok := objectState.LocalState().(configOrRow)
		if !ok {
			continue
		}
		if err := m.visitStorageInputTables(config, config.GetContent(), m.replacePlaceholderWithDefaultBucket); err != nil {
			warnings.Append(err)
		}
	}

	// Log errors as warning
	if warnings.Len() > 0 {
		m.logger.Warn(errors.Format(errors.PrefixError(warnings, "warning"), errors.FormatAsSentences()))
	}

	// Process renamed objects
	errs := errors.NewMultiError()
	if len(changes.Renamed()) > 0 {
		if err := m.onObjectsRename(changes.Renamed(), m.state.LocalObjects()); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
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
	branchState := m.state.MustGet(targetConfig.BranchKey())

	// Get key by path
	path := filesystem.Join(branchState.Path(), splitSource[0])
	sourceConfigState, found := m.state.GetByPath(path)
	if !found || !sourceConfigState.HasLocalState() {
		return errors.Errorf(
			`%s contains table "%s" in input mapping referencing to a non-existing configuration`,
			targetConfig.Desc(),
			inputTableSource)
	}
	sourceConfig := sourceConfigState.LocalState().(*model.Config)

	defaultBucket, found := m.state.Components().GetDefaultBucketByComponentId(sourceConfig.ComponentId, sourceConfig.Id)
	if !found {
		return nil
	}

	inputTable.Set(`source`, fmt.Sprintf("%s.%s", defaultBucket, splitSource[1]))
	markUsedInInputMapping(sourceConfig, targetConfig)
	return nil
}
