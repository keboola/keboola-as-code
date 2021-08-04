package plan

import (
	"fmt"
	"path/filepath"

	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

// Persist creates a plan for persist new/deleted objects
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
	// New configs
	for _, path := range b.UntrackedDirs() {
		for _, branch := range b.Branches() {
			if actions := b.tryAddConfig(path, branch); actions != nil {
				b.actions = append(b.actions, actions...)
				break
			}
		}
	}

	// New config rows from existing configs
	for _, path := range b.UntrackedDirs() {
		for _, config := range b.Configs() {
			if action := b.tryAddConfigRow(path, config.RelativePath(), config.ConfigKey); action != nil {
				b.actions = append(b.actions, action)
				break
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
}

func (b *persistPlanBuilder) tryAddConfig(projectPath string, branch *model.BranchState) []PersistAction {
	// Is path from the branch dir?
	relPath, err := filepath.Rel(branch.RelativePath(), projectPath)
	if err != nil {
		return nil
	}

	// Is config path matching naming template?
	matched, matches := b.Naming().Config.MatchPath(relPath)
	if !matched {
		return nil
	}

	// Get component ID
	componentId, ok := matches["component_id"]
	if !ok || componentId == "" {
		b.errors.Append(fmt.Errorf(`config's component id cannot be determined, path: "%s", path template: "%s"`, projectPath, b.Naming().Config))
		return nil
	}

	// Create action
	configKey := model.ConfigKey{BranchId: branch.Id, ComponentId: componentId}
	actions := make([]PersistAction, 0)
	action := &NewConfigAction{Key: configKey, Path: relPath, ProjectPath: projectPath}
	actions = append(actions, action)

	// Search for config rows
	for _, path := range b.UntrackedDirs() {
		if rowAction := b.tryAddConfigRow(path, projectPath, configKey); rowAction != nil {
			// Store row action inside config action too.
			// Config ID can be then set to the rows on invoke.
			action.Rows = append(action.Rows, rowAction)
			actions = append(actions, rowAction)
		}
	}

	return actions
}

func (b *persistPlanBuilder) tryAddConfigRow(projectPath, configPath string, configKey model.ConfigKey) *NewRowAction {
	// Is path from the config dir?
	relPath, err := filepath.Rel(configPath, projectPath)
	if err != nil {
		return nil
	}

	// Is config row pat matching naming template?
	if matched, _ := b.Naming().ConfigRow.MatchPath(relPath); !matched {
		return nil
	}

	// Create action
	rowKey := model.ConfigRowKey{BranchId: configKey.BranchId, ComponentId: configKey.ComponentId, ConfigId: configKey.Id}
	return &NewRowAction{Key: rowKey, Path: relPath, ProjectPath: projectPath}
}
