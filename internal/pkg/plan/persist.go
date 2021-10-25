package plan

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Persist creates a plan to persist new/deleted objects from local filesystem.
func Persist(projectState *state.State) (*PersistPlan, error) {
	builder := &persistPlanBuilder{
		PersistPlan: &PersistPlan{},
		State:       projectState,
		errors:      utils.NewMultiError(),
	}
	builder.build()
	return builder.PersistPlan, builder.errors.ErrorOrNil()
}

type persistPlanBuilder struct {
	*PersistPlan
	*state.State
	errors *utils.Error
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
	records := b.Manifest().GetRecords()
	keys := append([]string(nil), records.Keys()...)
	for _, key := range keys {
		recordRaw, _ := records.Get(key)
		record := recordRaw.(model.Record)
		if record.State().IsNotFound() {
			b.addAction(&DeleteRecordAction{record})
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

	path := model.NewPathInProject(parent.Path(), objectPath)

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
	case *NewConfigAction:
		if action := b.tryAddConfigRow(path, parent.Key); action != nil {
			// Set ConfigId on config persist, now it is unknown
			parent.OnPersist = append(parent.OnPersist, func(parentKey model.ConfigKey) {
				action.Key.ConfigId = parentKey.Id
			})
			return true
		}
		if action := b.tryAddConfig(path, parent.Key); action != nil {
			// Set ConfigId on config persist, now it is unknown
			parent.OnPersist = append(parent.OnPersist, func(parentKey model.ConfigKey) {
				action.ParentConfig.Id = parentKey.Id
			})
			return true
		}
	}

	return false
}

func (b *persistPlanBuilder) tryAddConfig(path model.PathInProject, parentKey model.Key) *NewConfigAction {
	// Is config path matching naming template?
	componentId, err := b.Naming().MatchConfigPath(parentKey.Kind(), path)
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
	}

	// Create action
	action := &NewConfigAction{PathInProject: path, Key: configKey}

	// Set parent config key
	if k, ok := parentKey.(model.ConfigKey); ok {
		action.ParentConfig = &model.ConfigKeySameBranch{
			ComponentId: k.ComponentId,
			Id:          k.Id,
		}
	}

	b.addAction(action)
	return action
}

func (b *persistPlanBuilder) tryAddConfigRow(path model.PathInProject, configKey model.ConfigKey) *NewRowAction {
	component, err := b.State.Components().Get(*configKey.ComponentKey())
	if err != nil {
		b.errors.Append(err)
		return nil
	}

	if !b.Naming().MatchConfigRowPath(component, path) {
		return nil
	}

	// Create action
	rowKey := model.ConfigRowKey{BranchId: configKey.BranchId, ComponentId: configKey.ComponentId, ConfigId: configKey.Id}
	action := &NewRowAction{PathInProject: path, Key: rowKey}
	b.addAction(action)
	return action
}

func (b *persistPlanBuilder) addAction(action PersistAction) {
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
