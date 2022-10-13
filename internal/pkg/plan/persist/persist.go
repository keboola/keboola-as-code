package persist

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// NewPlan creates a plan to persist new/deleted objects from local filesystem.
func NewPlan(projectState *state.State) (*Plan, error) {
	builder := &persistPlanBuilder{
		Plan:   &Plan{},
		State:  projectState,
		errors: errors.NewMultiError(),
	}
	builder.build()
	return builder.Plan, builder.errors.ErrorOrNil()
}

type persistPlanBuilder struct {
	*Plan
	*state.State
	errors errors.MultiError
}

func (b *persistPlanBuilder) build() {
	// Process children of the existing objects
	paths := b.State.PathsState() // clone paths state
	for _, path := range paths.UntrackedDirs() {
		if paths.IsTracked(path) {
			// path is already tracked
			// it is some new object's sub dir
			continue
		}

		for _, parent := range b.All() {
			if b.tryAdd(path, parent) {
				paths.MarkSubPathsTracked(path)
			}
		}
	}

	// Deleted objects
	for _, objectManifest := range b.Manifest().All() {
		if objectManifest.State().IsNotFound() {
			b.addAction(&deleteManifestRecordAction{objectManifest})
		}
	}

	// Sort actions by action order AND path
	sort.SliceStable(b.actions, func(i, j int) bool {
		iAction := b.actions[i]
		jAction := b.actions[j]
		orderDiff := iAction.Order() - jAction.Order()
		if orderDiff != 0 {
			return orderDiff < 0
		}
		return iAction.Path() < jAction.Path()
	})
}

func (b *persistPlanBuilder) tryAdd(fullPath string, parent model.RecordPaths) bool {
	// Is path from the parent?
	parentPath := parent.Path()
	if !filesystem.IsFrom(fullPath, parentPath) {
		return false
	}
	objectPath, err := filesystem.Rel(parentPath, fullPath)
	if err != nil {
		b.errors.Append(err)
		return false
	}

	path := model.NewAbsPath(parent.Path(), objectPath)

	// Add object according to the parent type
	switch parent := parent.(type) {
	case *model.BranchState:
		if b.tryAddConfig(path, parent.BranchKey) != nil {
			return true
		}
	case *model.ConfigState:
		if b.tryAddConfigRow(path, parent.ConfigKey) != nil {
			return true
		}
		if b.tryAddConfig(path, parent.ConfigKey) != nil {
			return true
		}
	case *model.ConfigRowState:
		if b.tryAddConfig(path, parent.ConfigRowKey) != nil {
			return true
		}
	case *newObjectAction:
		if parentKey, ok := parent.Key.(model.ConfigKey); ok {
			if action := b.tryAddConfigRow(path, parentKey); action != nil {
				// Set ConfigId on config persist, now it is unknown
				parent.OnPersist = append(parent.OnPersist, func(parentKey model.Key) {
					parentConfigKey := parentKey.(model.ConfigKey)
					key := action.Key.(model.ConfigRowKey)
					key.ConfigId = parentConfigKey.Id
					action.ParentKey = parentConfigKey
					action.Key = key
				})
				return true
			}
		}
		if action := b.tryAddConfig(path, parent.Key); action != nil {
			// Set ConfigId on config persist, now it is unknown
			parent.OnPersist = append(parent.OnPersist, func(parentKey model.Key) {
				action.ParentKey = parentKey
			})
			return true
		}
	}

	return false
}

func (b *persistPlanBuilder) tryAddConfig(path model.AbsPath, parentKey model.Key) *newObjectAction {
	// Is config path matching naming template?
	componentId, err := b.PathMatcher().MatchConfigPath(parentKey, path)
	if err != nil {
		b.errors.Append(err)
		return nil
	} else if componentId == "" {
		return nil
	}

	// Create key
	var configKey model.ConfigKey
	switch k := parentKey.(type) {
	case model.BranchKey:
		configKey = model.ConfigKey{BranchId: k.Id, ComponentId: componentId}
	case model.ConfigKey:
		configKey = model.ConfigKey{BranchId: k.BranchId, ComponentId: componentId}
	case model.ConfigRowKey:
		configKey = model.ConfigKey{BranchId: k.BranchId, ComponentId: componentId}
	default:
		panic(errors.Errorf(`unexpected parent key type "%T"`, parentKey))
	}

	// Create action
	action := &newObjectAction{AbsPath: path, Key: configKey, ParentKey: parentKey}

	b.addAction(action)
	return action
}

func (b *persistPlanBuilder) tryAddConfigRow(path model.AbsPath, parentKey model.ConfigKey) *newObjectAction {
	component, err := b.State.Components().GetOrErr(parentKey.ComponentId)
	if err != nil {
		b.errors.Append(err)
		return nil
	}

	if !b.PathMatcher().MatchConfigRowPath(component, path) {
		return nil
	}

	// Create action
	rowKey := model.ConfigRowKey{BranchId: parentKey.BranchId, ComponentId: parentKey.ComponentId, ConfigId: parentKey.Id}
	action := &newObjectAction{AbsPath: path, Key: rowKey, ParentKey: parentKey}
	b.addAction(action)
	return action
}

func (b *persistPlanBuilder) addAction(action action) {
	b.actions = append(b.actions, action)

	// Process children of the new object
	if parent, ok := action.(model.RecordPaths); ok {
		paths := b.State.PathsState() // clone paths state
		for _, path := range paths.UntrackedDirsFrom(parent.Path()) {
			if paths.IsTracked(path) {
				// path is already tracked
				// it is some new object's sub dir
				continue
			}

			if b.tryAdd(path, parent) {
				paths.MarkSubPathsTracked(path)
			}
		}
	}
}
