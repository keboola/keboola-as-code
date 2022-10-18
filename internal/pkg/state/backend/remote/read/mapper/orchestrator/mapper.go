package orchestrator

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

func (m *mapper) AfterRemoteOperation(_ context.Context, changes *model.RemoteChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.RemoteObjects()
	for _, objectState := range changes.Loaded() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			m.onRemoteLoad(configState.Remote, configState.ConfigManifest, allObjects)
		}
	}
	return errs.ErrorOrNil()
}

func (m *mapper) onRemoteLoad(config *model.Config, manifest *model.ConfigManifest, allObjects model.Objects) {
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
		m.logger.Warn(`Warning: `, errors.PrefixErrorf(err, `invalid orchestrator %s`, config.Desc()))
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

	// Set paths if parent path is set
	if l.manifest.Path() != "" {
		phasesDir := l.NamingGenerator().PhasesDir(l.manifest.Path())
		for _, phase := range l.config.Orchestration.Phases {
			if path, found := l.GetPath(phase.Key()); found {
				phase.AbsPath = path
			} else {
				phase.AbsPath = l.NamingGenerator().PhasePath(phasesDir, phase)
			}
			for _, task := range phase.Tasks {
				if path, found := l.GetPath(task.Key()); found {
					task.AbsPath = path
				} else {
					task.AbsPath = l.NamingGenerator().TaskPath(phase.Path(), task)
				}
			}
		}
	}

	return l.errors.ErrorOrNil()
}

func (l *remoteLoader) getPhases() ([]interface{}, error) {
	phasesRaw, found := l.config.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil, nil
	}
	phases, ok := phasesRaw.([]interface{})
	if !ok {
		return nil, errors.Errorf(`missing "%s" key`, model.OrchestratorPhasesContentKey)
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
		return nil, errors.Errorf(`missing "%s" key`, model.OrchestratorTasksContentKey)
	}
	l.config.Content.Delete(model.OrchestratorTasksContentKey)
	return tasks, nil
}

func (l *remoteLoader) parsePhase(phaseRaw interface{}) (*model.Phase, string, []string, error) {
	errs := errors.NewMultiError()
	content, ok := phaseRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil, "", nil, errors.New(`phase must be JSON object`)
	}

	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
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

func (l *remoteLoader) parseTask(taskRaw interface{}) error {
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
	phaseId, err := parser.phaseId()
	if err != nil {
		errs.Append(err)
	}

	// Component ID
	task.ComponentId, err = parser.componentId()
	if err != nil {
		errs.Append(err)
	}

	// ConfigId / ConfigData
	if len(task.ComponentId) > 0 {
		if parser.hasConfigId() {
			task.ConfigId, err = parser.configId()
			if err != nil {
				errs.Append(err)
			}
		} else if parser.hasConfigData() {
			task.ConfigData, err = parser.configData()
			if err != nil {
				errs.Append(err)
			}
		} else if task.Enabled {
			errs.Append(errors.New("task.configId, or task.configData and task.componentId must be specified"))
		}
	}

	// Get target config
	targetConfig, err := l.getTargetConfig(task.ComponentId, task.ConfigId)
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
		if phase, found := l.phaseByKey[cast.ToString(phaseId)]; found {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errs.Append(errors.Errorf(`phase "%d" not found`, phaseId))
		}
	}

	return errs.ErrorOrNil()
}

func (l *remoteLoader) getTargetConfig(componentId storageapi.ComponentID, configId storageapi.ConfigID) (*model.Config, error) {
	if len(componentId) == 0 || len(configId) == 0 {
		return nil, nil
	}

	configKey := model.ConfigKey{
		BranchId:    l.config.BranchId,
		ComponentId: componentId,
		Id:          configId,
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
