package orchestration

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type remoteLoadContext struct {
	state        *remote.State
	phasesSorter *phasesSorter
	orchestrator *model.Config
	errors       *errors.MultiError
}

func (m *orchestratorRemoteMapper) AfterRemoteOperation(changes *model.Changes) error {
	errs := errors.NewMultiError()
	for _, object := range changes.Loaded() {
		if ok, err := m.isOrchestrator(object.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			m.onRemoteLoad(object.(*model.Config))
		}
	}
	return errs.ErrorOrNil()
}

func (m *orchestratorRemoteMapper) onRemoteLoad(orchestrator *model.Config) {
	loader := &remoteLoadContext{
		state:        m.state,
		phasesSorter: newPhasesSorter(),
		orchestrator: orchestrator,
		errors:       errors.NewMultiError(),
	}
	if err := loader.load(); err != nil {
		// Convert errors to warning
		m.logger.Warn(`Warning: `, errors.PrefixError(fmt.Sprintf(`invalid orchestrator %s`, orchestrator.String()), err))
	}
}

func (l *remoteLoadContext) load() error {
	// Get phases
	phases, err := l.getPhases()
	if err != nil {
		l.errs.Append(err)
	}

	// Get tasks
	tasks, err := l.getTasks()
	if err != nil {
		l.errs.Append(err)
	}

	// Parse phases
	for apiIndex, phaseRaw := range phases {
		if phase, id, dependsOn, err := l.parsePhase(phaseRaw); err == nil {
			l.phasesSorter.addPhase(id, phase, dependsOn)
		} else {
			l.errs.Append(errors.PrefixError(fmt.Sprintf(`invalid phase[%d]`, apiIndex), err))
		}
	}

	// Parse tasks
	for apiIndex, taskRaw := range tasks {
		if err := l.parseTask(taskRaw); err != nil {
			l.errs.Append(errors.PrefixError(fmt.Sprintf(`invalid task[%d]`, apiIndex), err))
		}
	}

	// Sort phases by dependencies
	sortedPhases, err := l.phasesSorter.sortPhases()
	if err != nil {
		l.errs.Append(err)
	}

	// Set value
	l.orchestrator.Orchestration = &model.Orchestration{
		Phases: sortedPhases,
	}

	return l.errs.ErrorOrNil()
}

func (l *remoteLoadContext) getPhases() ([]interface{}, error) {
	phasesRaw, found := l.orchestrator.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil, nil
	}
	phases, ok := phasesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf(`missing "%s" key`, model.OrchestratorPhasesContentKey)
	}
	l.orchestrator.Content.Delete(model.OrchestratorPhasesContentKey)
	return phases, nil
}

func (l *remoteLoadContext) getTasks() ([]interface{}, error) {
	tasksRaw, found := l.orchestrator.Content.Get(model.OrchestratorTasksContentKey)
	if !found {
		return nil, nil
	}
	tasks, ok := tasksRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf(`missing "%s" key`, model.OrchestratorTasksContentKey)
	}
	l.orchestrator.Content.Delete(model.OrchestratorTasksContentKey)
	return tasks, nil
}

func (l *remoteLoadContext) parsePhase(phaseRaw interface{}) (*model.Phase, string, []string, error) {
	errs := errors.NewMultiError()
	content, ok := phaseRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil, "", nil, fmt.Errorf(`phase must be JSON object`)
	}

	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchId:    l.orchestrator.BranchId,
			ComponentId: l.orchestrator.ComponentId,
			ConfigId:    l.orchestrator.ConfigId,
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
		for _, dependsOnId := range dependsOnIds {
			dependsOn = append(dependsOn, cast.ToString(dependsOnId))
		}
	} else {
		errs.Append(err)
	}

	// Additional content
	phase.Content = parser.additionalContent()
	return phase, cast.ToString(id), dependsOn, errs.ErrorOrNil()
}

func (l *remoteLoadContext) parseTask(taskRaw interface{}) error {
	errs := errors.NewMultiError()
	content, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return fmt.Errorf(`task must be JSON object`)
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

	// Get phase
	phaseId, err := parser.phaseId()
	if err != nil {
		errs.Append(err)
	}

	// Component ID
	task.ComponentId, err = parser.componentId()
	if err != nil {
		errs.Append(err)
	}

	// Config ID
	if len(task.ComponentId) > 0 {
		task.ConfigId, err = parser.configId()
		if err != nil {
			errs.Append(err)
		}
	}

	// Get target config
	targetConfig, err := l.getTargetConfig(task.ComponentId, task.ConfigId)
	if err != nil {
		errs.Append(err)
	} else if targetConfig != nil {
		markConfigUsedInOrchestrator(targetConfig, l.orchestrator)
	}

	// Additional content
	task.Content = parser.additionalContent()

	// Get phase
	if errors.Len() == 0 {
		if phase, found := l.phasesSorter.phaseByKey[cast.ToString(phaseId)]; found {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errs.Append(fmt.Errorf(`phase "%d" not found`, phaseId))
		}
	}

	return errs.ErrorOrNil()
}

func (l *remoteLoadContext) getTargetConfig(componentId model.ComponentId, configId model.ConfigId) (*model.Config, error) {
	if len(componentId) == 0 || len(configId) == 0 {
		return nil, nil
	}

	configKey := model.ConfigKey{
		BranchId:    l.orchestrator.BranchId,
		ComponentId: componentId,
		ConfigId:    configId,
	}

	configRaw, found := l.state.Get(configKey)
	if !found {
		return nil, fmt.Errorf(`%s not found`, configKey.String())
	}

	config, ok := configRaw.(*model.Config)
	if !ok {
		return nil, fmt.Errorf(`expected %s, found %s`, configKey.String(), configRaw.String())
	}

	return config, nil
}
