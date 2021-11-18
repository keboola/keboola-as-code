package orchestrator

import (
	"fmt"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestratorConfigKey(recipe.InternalObject.Key()); err != nil {
		m.Logger.Warn(`Warning: `, err)
		return nil
	} else if !ok {
		return nil
	}

	loader := &remoteLoader{
		MapperContext: m.MapperContext,
		phasesSorter:  newPhasesSorter(),
		config:        recipe.InternalObject.(*model.Config),
		manifest:      recipe.Manifest.(*model.ConfigManifest),
		errors:        utils.NewMultiError(),
	}
	return loader.load()
}

type remoteLoader struct {
	model.MapperContext
	*phasesSorter
	config   *model.Config
	manifest *model.ConfigManifest
	errors   *utils.Error
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
			l.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid phase[%d]`, apiIndex), err))
		}
	}

	// Parse tasks
	for apiIndex, taskRaw := range tasks {
		if err := l.parseTask(taskRaw); err != nil {
			l.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid task[%d]`, apiIndex), err))
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

	// Set paths if parent path is set
	if l.manifest.Path() != "" {
		phasesDir := l.Naming.PhasesDir(l.manifest.Path())
		for _, phase := range l.config.Orchestration.Phases {
			if path, found := l.Naming.GetCurrentPath(phase.Key()); found {
				phase.PathInProject = path
			} else {
				phase.PathInProject = l.Naming.PhasePath(phasesDir, phase)
			}
			for _, task := range phase.Tasks {
				if path, found := l.Naming.GetCurrentPath(task.Key()); found {
					task.PathInProject = path
				} else {
					task.PathInProject = l.Naming.TaskPath(phase.Path(), task)
				}
			}
		}
	}

	// Convert errors to warning
	if l.errors.Len() > 0 {
		l.Logger.Warn(`Warning: `, utils.PrefixError(fmt.Sprintf(`invalid orchestrator %s`, l.config.Desc()), l.errors))
	}

	return nil
}

func (l *remoteLoader) getPhases() ([]interface{}, error) {
	phasesRaw, found := l.config.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil, nil
	}
	phases, ok := phasesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf(`missing "%s" key`, model.OrchestratorPhasesContentKey)
	}
	l.config.Content.Delete(model.OrchestratorPhasesContentKey)
	return phases, nil
}

func (l *remoteLoader) getTasks() ([]interface{}, error) {
	tasksRaw, found := l.config.Content.Get(model.OrchestratorTasksContentKey)
	if !found {
		return nil, nil
	}
	tasks, ok := tasksRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf(`missing "%s" key`, model.OrchestratorTasksContentKey)
	}
	l.config.Content.Delete(model.OrchestratorTasksContentKey)
	return tasks, nil
}

func (l *remoteLoader) parsePhase(phaseRaw interface{}) (*model.Phase, string, []string, error) {
	errors := utils.NewMultiError()
	content, ok := phaseRaw.(orderedmap.OrderedMap)
	if !ok {
		return nil, "", nil, fmt.Errorf(`phase must be JSON object`)
	}

	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
		},
	}
	parser := &phaseParser{content: &content}

	// Get ID
	id, err := parser.id()
	if err != nil {
		errors.Append(err)
	}

	// Get name
	phase.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get dependsOn
	var dependsOn []string
	dependsOnIds, err := parser.dependsOnIds()
	if err == nil {
		for _, dependsOnId := range dependsOnIds {
			dependsOn = append(dependsOn, cast.ToString(dependsOnId))
		}
	} else {
		errors.Append(err)
	}

	// Additional content
	phase.Content = parser.additionalContent()
	return phase, cast.ToString(id), dependsOn, errors.ErrorOrNil()
}

func (l *remoteLoader) parseTask(taskRaw interface{}) error {
	errors := utils.NewMultiError()
	content, ok := taskRaw.(orderedmap.OrderedMap)
	if !ok {
		return fmt.Errorf(`task must be JSON object`)
	}

	task := &model.Task{}
	parser := &taskParser{content: &content}

	// Get ID
	_, err := parser.id()
	if err != nil {
		errors.Append(err)
	}

	// Get name
	task.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get phase
	phaseId, err := parser.phaseId()
	if err != nil {
		errors.Append(err)
	}

	// Component ID
	task.ComponentId, err = parser.componentId()
	if err != nil {
		errors.Append(err)
	}

	// Config ID
	if len(task.ComponentId) > 0 {
		task.ConfigId, err = parser.configId()
		if err != nil {
			errors.Append(err)
		}
	}

	// Additional content
	task.Content = parser.additionalContent()

	// Get phase
	if errors.Len() == 0 {
		if phase, found := l.phaseByKey[cast.ToString(phaseId)]; found {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errors.Append(fmt.Errorf(`phase "%d" not found`, phaseId))
		}
	}

	return errors.ErrorOrNil()
}
