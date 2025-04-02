package persist

import (
	"context"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// NewPlan creates a plan to persist new/deleted objects from local filesystem.
func NewPlan(ctx context.Context, projectState *state.State) (*Plan, error) {
	builder := &persistPlanBuilder{
		Plan:   &Plan{},
		State:  projectState,
		errors: errors.NewMultiError(),
	}
	builder.build(ctx)
	return builder.Plan, builder.errors.ErrorOrNil()
}

type persistPlanBuilder struct {
	*Plan
	*state.State
	errors errors.MultiError
}

func (b *persistPlanBuilder) build(ctx context.Context) {
	// Process children of the existing objects
	paths := b.PathsState() // clone paths state
	for _, path := range paths.UntrackedDirs(ctx) {
		if paths.IsTracked(path) {
			// path is already tracked
			// it is some new object's sub dir
			continue
		}

		for _, parent := range b.All() {
			if b.tryAdd(ctx, path, parent) {
				paths.MarkSubPathsTracked(path)
			}
		}
	}

	// Deleted objects
	for _, objectManifest := range b.Manifest().All() {
		if objectManifest.State().IsNotFound() {
			b.addAction(ctx, &deleteManifestRecordAction{objectManifest})
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

func (b *persistPlanBuilder) tryAdd(ctx context.Context, fullPath string, parent model.RecordPaths) bool {
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
		if b.tryAddConfig(ctx, path, parent.BranchKey) != nil {
			return true
		}
	case *model.ConfigState:
		if b.tryAddConfigRow(ctx, path, parent.ConfigKey) != nil {
			return true
		}
		if b.tryAddConfig(ctx, path, parent.ConfigKey) != nil {
			return true
		}
	case *model.ConfigRowState:
		if b.tryAddConfig(ctx, path, parent.ConfigRowKey) != nil {
			return true
		}
	case *newObjectAction:
		if parentKey, ok := parent.Key.(model.ConfigKey); ok {
			if action := b.tryAddConfigRow(ctx, path, parentKey); action != nil {
				// Set ConfigID on config persist, now it is unknown
				parent.OnPersist = append(parent.OnPersist, func(parentKey model.Key) {
					parentConfigKey := parentKey.(model.ConfigKey)
					key := action.Key.(model.ConfigRowKey)
					key.ConfigID = parentConfigKey.ID
					action.ParentKey = parentConfigKey
					action.Key = key
				})
				return true
			}
		}
		if action := b.tryAddConfig(ctx, path, parent.Key); action != nil {
			// Set ConfigID on config persist, now it is unknown
			parent.OnPersist = append(parent.OnPersist, func(parentKey model.Key) {
				action.ParentKey = parentKey
			})
			return true
		}
	}

	return false
}

func (b *persistPlanBuilder) tryAddConfig(ctx context.Context, path model.AbsPath, parentKey model.Key) *newObjectAction {
	// Is config path matching naming template?
	componentID, err := b.PathMatcher().MatchConfigPath(parentKey, path)
	if err != nil {
		b.errors.Append(err)
		return nil
	} else if componentID == "" {
		return nil
	}

	// Create key
	var configKey model.ConfigKey
	switch k := parentKey.(type) {
	case model.BranchKey:
		configKey = model.ConfigKey{BranchID: k.ID, ComponentID: componentID}
	case model.ConfigKey:
		configKey = model.ConfigKey{BranchID: k.BranchID, ComponentID: componentID}
	case model.ConfigRowKey:
		configKey = model.ConfigKey{BranchID: k.BranchID, ComponentID: componentID}
	default:
		panic(errors.Errorf(`unexpected parent key type "%T"`, parentKey))
	}

	// Create action
	action := &newObjectAction{AbsPath: path, Key: configKey, ParentKey: parentKey}

	b.addAction(ctx, action)
	return action
}

func (b *persistPlanBuilder) tryAddConfigRow(ctx context.Context, path model.AbsPath, parentKey model.ConfigKey) *newObjectAction {
	component, err := b.State.Components().GetOrErr(parentKey.ComponentID)
	if err != nil {
		b.errors.Append(err)
		return nil
	}

	if !b.PathMatcher().MatchConfigRowPath(component, path) {
		return nil
	}

	// Create action
	rowKey := model.ConfigRowKey{BranchID: parentKey.BranchID, ComponentID: parentKey.ComponentID, ConfigID: parentKey.ID}
	action := &newObjectAction{AbsPath: path, Key: rowKey, ParentKey: parentKey}
	b.addAction(ctx, action)
	return action
}

func (b *persistPlanBuilder) addAction(ctx context.Context, action action) {
	b.actions = append(b.actions, action)

	// Process children of the new object
	if parent, ok := action.(model.RecordPaths); ok {
		paths := b.PathsState() // clone paths state
		for _, path := range paths.UntrackedDirsFrom(ctx, parent.Path()) {
			if paths.IsTracked(path) {
				// path is already tracked
				// it is some new object's sub dir
				continue
			}

			if b.tryAdd(ctx, path, parent) {
				paths.MarkSubPathsTracked(path)
			}
		}
	}
}
