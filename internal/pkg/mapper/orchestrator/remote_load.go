package orchestrator

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *orchestratorMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.RemoteObjects()

	// Handle loaded orchestrators (pull operation)
	for _, objectState := range changes.Loaded() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			m.onRemoteLoad(ctx, configState.Remote, configState.ConfigManifest, allObjects)
			// Collect schedule data from scheduler configs before ignore mapper removes them
			m.collectSchedulesFromRemote(configState.Remote)
		}
	}

	// Handle saved orchestrators (push operation) - sync inline schedules with API
	for _, objectState := range changes.Saved() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			if err := m.syncSchedulesOnSave(ctx, configState); err != nil {
				errs.Append(err)
			}
		}
	}

	return errs.ErrorOrNil()
}

// collectSchedulesFromRemote finds scheduler configs targeting this orchestrator and stores
// their data in the orchestration's Schedules field. This must be called before the ignore
// mapper removes the scheduler configs from state.
func (m *orchestratorMapper) collectSchedulesFromRemote(orchestratorConfig *model.Config) {
	if orchestratorConfig.Orchestration == nil {
		return
	}

	var schedules []model.ScheduleYAML

	// Find all scheduler configs that target this orchestrator
	for _, objectState := range m.state.All() {
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		config := configState.RemoteState()
		if config == nil {
			continue
		}
		schedulerConfig := config.(*model.Config)

		// Check if this is a scheduler component
		component, err := m.state.Components().GetOrErr(schedulerConfig.ComponentID)
		if err != nil || !component.IsScheduler() {
			continue
		}

		// Check if this scheduler targets our orchestrator
		for _, rel := range schedulerConfig.Relations.GetByType(model.SchedulerForRelType) {
			schedulerFor := rel.(*model.SchedulerForRelation)
			if schedulerFor.ComponentID == orchestratorConfig.ComponentID && schedulerFor.ConfigID == orchestratorConfig.ID {
				// Convert to YAML format
				scheduleYAML := m.buildScheduleYAMLFromRemote(schedulerConfig)
				schedules = append(schedules, scheduleYAML)
				break
			}
		}
	}

	orchestratorConfig.Orchestration.Schedules = schedules
}

// buildScheduleYAMLFromRemote converts a scheduler config to ScheduleYAML.
func (m *orchestratorMapper) buildScheduleYAMLFromRemote(config *model.Config) model.ScheduleYAML {
	schedule := model.ScheduleYAML{
		Name: config.Name,
		// Include the scheduler config ID for tracking during push operations
		Keboola: &model.ScheduleKeboolaMeta{
			ConfigID: config.ID.String(),
		},
	}

	// Get description
	if config.Description != "" {
		schedule.Description = config.Description
	}

	// Get schedule (cron) details from the schedule key
	if config.Content != nil {
		if scheduleRaw, ok := config.Content.Get("schedule"); ok {
			if scheduleMap, ok := scheduleRaw.(*orderedmap.OrderedMap); ok {
				// Get cronTab
				if cronTab, ok := scheduleMap.Get("cronTab"); ok {
					if cronStr, ok := cronTab.(string); ok {
						schedule.Cron = cronStr
					}
				}
				// Get timezone
				if timezone, ok := scheduleMap.Get("timezone"); ok {
					if tzStr, ok := timezone.(string); ok {
						schedule.Timezone = tzStr
					}
				}
				// Get state (enabled/disabled)
				if state, ok := scheduleMap.Get("state"); ok {
					if stateStr, ok := state.(string); ok {
						enabled := stateStr == "enabled"
						schedule.Enabled = &enabled
					}
				}
			}
		}
	}

	return schedule
}

func (m *orchestratorMapper) onRemoteLoad(ctx context.Context, config *model.Config, manifest *model.ConfigManifest, allObjects model.Objects) {
	loader := &remoteLoader{
		State:        m.state,
		phasesSorter: newPhasesSorter(),
		allObjects:   allObjects,
		config:       config,
		manifest:     manifest,
		errors:       errors.NewMultiError(),
	}
	if err := loader.load(); err != nil {
		// Convert errors to warning
		err = errors.PrefixErrorf(err, `invalid orchestrator %s`, config.Desc())
		m.logger.Warn(ctx, errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
	}
}

type remoteLoader struct {
	*state.State
	*phasesSorter
	allObjects model.Objects
	config     *model.Config
	manifest   *model.ConfigManifest
	errors     errors.MultiError
}

func (l *remoteLoader) load() error {
	// Get phases
	phases, err := l.getPhases()
	if err != nil {
		l.errors.Append(err)
	}

	// Get tasks
	tasks, err := l.getTasks()
	if err != nil {
		l.errors.Append(err)
	}

	// Parse phases
	for apiIndex, phaseRaw := range phases {
		if phase, id, dependsOn, err := l.parsePhase(phaseRaw); err == nil {
			key := id
			l.phasesKeys = append(l.phasesKeys, key)
			l.phaseByKey[key] = phase
			l.phaseDependsOnKeys[key] = dependsOn
		} else {
			l.errors.AppendWithPrefixf(err, `invalid phase[%d]`, apiIndex)
		}
	}

	// Parse tasks
	for apiIndex, taskRaw := range tasks {
		if err := l.parseTask(taskRaw); err != nil {
			l.errors.AppendWithPrefixf(err, `invalid task[%d]`, apiIndex)
		}
	}

	// Sort phases by dependencies
	sortedPhases, err := l.sortPhases()
	if err != nil {
		l.errors.Append(err)
	}

	// Convert pointers to values
	l.config.Orchestration = &model.Orchestration{
		Phases: sortedPhases,
	}

	// Phases and tasks are now stored inline in _config.yml, so no paths are set.
	// The legacy phases directory structure is no longer used.

	return l.errors.ErrorOrNil()
}

func (l *remoteLoader) getPhases() ([]any, error) {
	phasesRaw, found := l.config.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil, nil
	}
	phases, ok := phasesRaw.([]any)
	if !ok {
		return nil, errors.Errorf(`missing "%s" key`, model.OrchestratorPhasesContentKey)
	}
	l.config.Content.Delete(model.OrchestratorPhasesContentKey)
	return phases, nil
}

func (l *remoteLoader) getTasks() ([]any, error) {
	tasksRaw, found := l.config.Content.Get(model.OrchestratorTasksContentKey)
	if !found {
		return nil, nil
	}
	tasks, ok := tasksRaw.([]any)
	if !ok {
		return nil, errors.Errorf(`missing "%s" key`, model.OrchestratorTasksContentKey)
	}
	l.config.Content.Delete(model.OrchestratorTasksContentKey)
	return tasks, nil
}

func (l *remoteLoader) parsePhase(phaseRaw any) (*model.Phase, string, []string, error) {
	errs := errors.NewMultiError()
	content, ok := phaseRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil, "", nil, errors.New(`phase must be JSON object`)
	}

	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchID:    l.config.BranchID,
			ComponentID: l.config.ComponentID,
			ConfigID:    l.config.ID,
		},
	}
	parser := &phaseParser{content: content}

	// Get ID
	id, err := parser.id()
	if err != nil {
		errs.Append(err)
	}

	// Get name
	phase.Name, err = parser.name()
	if err != nil {
		errs.Append(err)
	}

	// Get dependsOn
	var dependsOn []string
	dependsOnIds, err := parser.dependsOnIds()
	if err == nil {
		for _, dependsOnID := range dependsOnIds {
			dependsOn = append(dependsOn, cast.ToString(dependsOnID))
		}
	} else {
		errs.Append(err)
	}

	// Additional content
	phase.Content = parser.additionalContent()
	return phase, cast.ToString(id), dependsOn, errs.ErrorOrNil()
}

func (l *remoteLoader) parseTask(taskRaw any) error {
	errs := errors.NewMultiError()
	content, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return errors.New(`task must be JSON object`)
	}

	task := &model.Task{}
	parser := &taskParser{content: content}

	// Get ID
	_, err := parser.id()
	if err != nil {
		errs.Append(err)
	}

	// Get name
	task.Name, err = parser.name()
	if err != nil {
		errs.Append(err)
	}

	// Get enabled, optional field, default true
	task.Enabled, _ = parser.enabled()

	// Get phase
	phaseID, err := parser.phaseID()
	if err != nil {
		errs.Append(err)
	}

	// Component ID
	task.ComponentID, err = parser.componentID()
	if err != nil {
		errs.Append(err)
	}

	// ConfigID / ConfigData
	if len(task.ComponentID) > 0 {
		switch {
		case parser.hasConfigID():
			task.ConfigID, err = parser.configID()
			if err != nil {
				errs.Append(err)
			}
		case parser.hasConfigData():
			task.ConfigData, err = parser.configData()
			if err != nil {
				errs.Append(err)
			}
		case task.Enabled:
			errs.Append(errors.New("task.configId, or task.configData and task.componentId must be specified"))
		}
	}

	// Get target config
	targetConfig, err := l.getTargetConfig(task.ComponentID, task.ConfigID)
	if err != nil {
		errs.Append(err)
	} else if targetConfig != nil {
		task.ConfigPath = l.MustGet(targetConfig.Key()).Path()
		markConfigUsedInOrchestrator(targetConfig, l.config)
	}

	// Additional content
	task.Content = parser.additionalContent()

	// Get phase
	if errs.Len() == 0 {
		if phase, found := l.phaseByKey[cast.ToString(phaseID)]; found {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errs.Append(errors.Errorf(`phase "%s" not found`, phaseID))
		}
	}

	return errs.ErrorOrNil()
}

func (l *remoteLoader) getTargetConfig(componentID keboola.ComponentID, configID keboola.ConfigID) (*model.Config, error) {
	if len(componentID) == 0 || len(configID) == 0 {
		return nil, nil
	}

	configKey := model.ConfigKey{
		BranchID:    l.config.BranchID,
		ComponentID: componentID,
		ID:          configID,
	}

	configRaw, found := l.allObjects.Get(configKey)
	if !found {
		return nil, errors.Errorf(`%s not found`, configKey.Desc())
	}

	config, ok := configRaw.(*model.Config)
	if !ok {
		return nil, errors.Errorf(`expected %s, found %s`, configKey.Desc(), configRaw.Desc())
	}

	return config, nil
}

// syncSchedulesOnSave synchronizes inline schedules from the orchestrator's _config.yml
// with the Keboola Scheduler API when the orchestrator is pushed.
// Note: This function doesn't run during push anymore - schedule sync is disabled.
// Inline schedules are stored in _config.yml and synced on pull only.
func (m *orchestratorMapper) syncSchedulesOnSave(ctx context.Context, configState *model.ConfigState) error {
	// Schedule sync on push is disabled for now.
	// Users should manage schedules via the Keboola UI or API directly.
	// The inline schedules in _config.yml are read-only - they reflect the API state on pull.
	return nil
}

// updateSchedulerFromInlineByID updates an existing scheduler config directly via API using its config ID.
func (m *orchestratorMapper) updateSchedulerFromInlineByID(ctx context.Context, api *keboola.AuthorizedAPI, configID string, inlineSchedule *model.ScheduleYAML, orchestratorConfig *model.Config) error {
	// Build scheduler config content from inline schedule
	schedulerContent := m.buildSchedulerContent(inlineSchedule, orchestratorConfig)

	// Update via API using the config ID directly
	_, err := api.UpdateConfigRequest(&keboola.ConfigWithRows{
		Config: &keboola.Config{
			ConfigKey: keboola.ConfigKey{
				BranchID:    orchestratorConfig.BranchID,
				ComponentID: keboola.SchedulerComponentID,
				ID:          keboola.ConfigID(configID),
			},
			Name:          inlineSchedule.Name,
			Description:   inlineSchedule.Description,
			Content:       schedulerContent,
			ChangeDescription: "Updated from inline schedule in orchestrator",
		},
	}, []string{"name", "description", "configuration"}).Send(ctx)

	return err
}

// buildSchedulerContent builds the scheduler config content from inline schedule data.
// This is kept for potential future use when schedule sync is re-enabled.
func (m *orchestratorMapper) buildSchedulerContent(inlineSchedule *model.ScheduleYAML, orchestratorConfig *model.Config) *orderedmap.OrderedMap {
	content := orderedmap.New()

	// Build schedule section
	schedule := orderedmap.New()
	schedule.Set("cronTab", inlineSchedule.Cron)
	if inlineSchedule.Timezone != "" {
		schedule.Set("timezone", inlineSchedule.Timezone)
	} else {
		schedule.Set("timezone", "UTC")
	}
	if inlineSchedule.Enabled != nil && *inlineSchedule.Enabled {
		schedule.Set("state", "enabled")
	} else {
		schedule.Set("state", "disabled")
	}
	content.Set("schedule", schedule)

	// Build target section
	target := orderedmap.New()
	target.Set("componentId", orchestratorConfig.ComponentID.String())
	target.Set("configurationId", orchestratorConfig.ID.String())
	target.Set("mode", "run")
	content.Set("target", target)

	return content
}
