package defaultbucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type mapper struct {
	logger log.Logger
	state  *state.State
}

type dependencies interface {
	Logger() log.Logger
}

func NewMapper(state *state.State, d dependencies) *mapper {
	return &mapper{state: state, logger: d.Logger()}
}

type configOrRow interface {
	model.ObjectWithContent
	BranchKey() model.BranchKey
}

// AfterLocalOperation - replace placeholders with default buckets in IM.
func (m *mapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
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
		m.logger.Warn(errors.PrefixError(warnings, "Warning"))
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

func (m *mapper) replacePlaceholderWithDefaultBucket(
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

// onObjectsRename - find renamed configurations that are used in default buckets placeholders.
func (m *mapper) onObjectsRename(renamed []model.RenameAction, allObjects model.Objects) error {
	// Find renamed configurations used in IM.
	objectsToUpdate := make(map[string]model.Key)
	for _, object := range renamed {
		manifest, ok := object.Manifest.(*model.ConfigManifest)
		if !ok {
			continue
		}

		localConfigRaw, found := allObjects.Get(manifest.Key())
		if !found {
			continue
		}
		localConfig := localConfigRaw.(*model.Config)

		for _, relationRaw := range localConfig.Relations.GetByType(model.UsedInConfigInputMappingRelType) {
			relation := relationRaw.(*model.UsedInConfigInputMappingRelation)
			objectsToUpdate[relation.UsedIn.String()] = relation.UsedIn
		}
		for _, relationRaw := range localConfig.Relations.GetByType(model.UsedInRowInputMappingRelType) {
			relation := relationRaw.(*model.UsedInRowInputMappingRelation)
			objectsToUpdate[relation.UsedIn.String()] = relation.UsedIn
		}
	}

	// Log and save
	uow := m.state.LocalManager().NewUnitOfWork(context.Background())
	errs := errors.NewMultiError()
	if len(objectsToUpdate) > 0 {
		m.logger.Debug(`Need to update configurations:`)
		for _, key := range objectsToUpdate {
			m.logger.Debugf(`  - %s`, key.Desc())
			objectState := m.state.MustGet(key)
			uow.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields(`configuration`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errs.Append(err)
	}

	return errs.ErrorOrNil()
}
