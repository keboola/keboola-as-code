package plan

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Persist creates a plan to persist new/deleted objects from local filesystem.
func Persist(projectState *state.State) *PersistPlan {
	builder := &persistPlanBuilder{State: projectState, PersistPlan: &PersistPlan{}, errors: utils.NewMultiError()}
	builder.build()
	return builder.PersistPlan
}

type persistPlanBuilder struct {
	*state.State
	*PersistPlan
	errors *utils.Error
}

func (b *persistPlanBuilder) build() {
	// Process children of the existing objects
	for _, path := range b.UntrackedDirs() {
		for _, parent := range b.All() {
			b.tryAdd(path, parent)
		}
	}

	// Process children of the new objects
	for _, path := range b.UntrackedDirs() {
		actions := b.actions
		for _, parent := range actions {
			if parent, ok := parent.(model.RecordPaths); ok {
				b.tryAdd(path, parent)
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
			b.actions = append(b.actions, &DeleteRecordAction{Record: record})
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

func (b *persistPlanBuilder) tryAdd(fullPath string, parent model.RecordPaths) {
	// Is path from the parent?
	parentPath := parent.Path()
	if !filesystem.IsFrom(fullPath, parentPath) {
		return
	}
	objectPath, err := filesystem.Rel(parentPath, fullPath)
	if err != nil {
		b.errors.Append(err)
		return
	}
	path := model.NewPathInProject(parent.Path(), objectPath)

	// Add object according to the parent type
	switch p := parent.(type) {
	case *model.BranchState:
		if b.tryAddConfig(path, p.BranchKey) != nil {
			return
		}
	case *model.ConfigState:
		if b.tryAddConfigRow(path, p.ConfigKey) != nil {
			return
		}
		if b.tryAddVariables(path, &p.ConfigKey) != nil {
			return
		}
	case *NewConfigAction:
		if action := b.tryAddConfigRow(path, p.Key); action != nil {
			// Set ConfigId on config persist, now it is unknown
			p.OnPersist = append(p.OnPersist, func(parentKey model.ConfigKey) {
				action.Key.ConfigId = parentKey.Id
			})
			return
		}
		if b.tryAddVariables(path, &p.Key) != nil {
			return
		}
	}
}

func (b *persistPlanBuilder) tryAddConfig(path model.PathInProject, branchKey model.BranchKey) *NewConfigAction {
	// Does the path match the naming template?
	matched, matches := b.Naming().Config.MatchPath(path.ObjectPath)
	if !matched {
		return nil
	}

	// Get component ID
	componentId, ok := matches["component_id"]
	if !ok || componentId == "" {
		b.errors.Append(fmt.Errorf(`config's component id cannot be determined, path: "%s", path template: "%s"`, path.Path(), b.Naming().Config))
		return nil
	}

	// Action
	action := &NewConfigAction{
		PathInProject: path,
		Key:           model.ConfigKey{BranchId: branchKey.Id, ComponentId: componentId},
	}
	b.addAction(action)
	return action
}

func (b *persistPlanBuilder) tryAddVariables(path model.PathInProject, parentKey *model.ConfigKey) *NewConfigAction {
	// Does the path match the naming template?
	if matched, _ := b.Naming().Variables.MatchPath(path.ObjectPath); !matched {
		return nil
	}

	// Config action
	action := &NewConfigAction{
		PathInProject: path,
		Key:           model.ConfigKey{BranchId: parentKey.BranchId, ComponentId: model.VariablesComponentId},
	}
	b.addAction(action)

	// Relation action
	// Note: keys are pointers, so both IDs will be set during execution
	relation := &NewVariablesRelAction{Variables: &action.Key, ConfigKey: parentKey}
	b.addAction(relation)

	return action
}

func (b *persistPlanBuilder) tryAddConfigRow(path model.PathInProject, configKey model.ConfigKey) *NewRowAction {
	// Is config row path matching naming template?
	if matched, _ := b.Naming().ConfigRow.MatchPath(path.ObjectPath); !matched {
		return nil
	}

	// Action
	action := &NewRowAction{
		PathInProject: path,
		Key:           model.ConfigRowKey{BranchId: configKey.BranchId, ComponentId: configKey.ComponentId, ConfigId: configKey.Id},
	}
	b.addAction(action)

	return action
}

func (b *persistPlanBuilder) addAction(action PersistAction) {
	b.actions = append(b.actions, action)
}
