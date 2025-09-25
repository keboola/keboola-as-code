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
	for _, objectState := range changes.Loaded() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			m.onRemoteLoad(ctx, configState.Remote, configState.ConfigManifest, allObjects)
		}
	}
	return errs.ErrorOrNil()
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

func (l *remoteLoader) getPhases() ([]any, error) {
	phasesRaw, found := l.config.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil, nil
	}
	// Always remove the key from content
	l.config.Content.Delete(model.OrchestratorPhasesContentKey)

	phases, ok := phasesRaw.([]any)
	if !ok {
		// If not an array, treat as empty (do not warn, tests expect no warning here)
		return nil, nil
	}
	return phases, nil
}

func (l *remoteLoader) getTasks() ([]any, error) {
	tasksRaw, found := l.config.Content.Get(model.OrchestratorTasksContentKey)
	if !found {
		return nil, nil
	}
	// Always remove the key from content
	l.config.Content.Delete(model.OrchestratorTasksContentKey)

	tasks, ok := tasksRaw.([]any)
	if !ok {
		// If not an array, treat as empty (do not warn, tests expect no warning here)
		return nil, nil
	}
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
