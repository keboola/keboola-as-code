package orchestrator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"v.io/x/lib/toposort"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestratorConfig(recipe.InternalObject); err != nil {
		m.Logger.Warn(`Warning: `, err)
		return nil
	} else if !ok {
		return nil
	}

	loader := &remoteLoader{
		MapperContext:  m.MapperContext,
		config:         recipe.InternalObject.(*model.Config),
		phaseIdByIndex: make(map[int]string),
		phaseById:      make(map[string]*model.Phase),
		errors:         utils.NewMultiError(),
	}
	return loader.load()
}

type remoteLoader struct {
	model.MapperContext
	config         *model.Config
	phases         []*model.Phase
	phaseIdByIndex map[int]string
	phaseById      map[string]*model.Phase
	errors         *utils.Error
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
		if phase, id, err := l.parsePhase(phaseRaw); err == nil {
			index := len(l.phases)
			l.phases = append(l.phases, phase)
			l.phaseIdByIndex[index] = id
			l.phaseById[id] = phase
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
	l.config.Orchestration = &model.Orchestration{}
	for _, phase := range sortedPhases {
		l.config.Orchestration.Phases = append(l.config.Orchestration.Phases, *phase)
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

func (l *remoteLoader) parsePhase(phaseRaw interface{}) (*model.Phase, string, error) {
	errors := utils.NewMultiError()
	content, ok := phaseRaw.(orderedmap.OrderedMap)
	if !ok {
		return nil, "", fmt.Errorf(`phase must be JSON object`)
	}

	phase := &model.Phase{}
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

	// Additional content
	phase.Content = parser.additionalContent()
	return phase, cast.ToString(id), errors.ErrorOrNil()
}

func (l *remoteLoader) parseTask(taskRaw interface{}) error {
	errors := utils.NewMultiError()
	content, ok := taskRaw.(orderedmap.OrderedMap)
	if !ok {
		return fmt.Errorf(`task must be JSON object`)
	}

	task := model.Task{}
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
		if phase, found := l.phaseById[cast.ToString(phaseId)]; found {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errors.Append(fmt.Errorf(`phase "%d" not found`, phaseId))
		}
	}

	return errors.ErrorOrNil()
}

func (l *remoteLoader) sortPhases() ([]*model.Phase, error) {
	errors := utils.NewMultiError()
	graph := &toposort.Sorter{}

	// Add dependencies to graph
	for index, phase := range l.phases {
		id := l.phaseIdByIndex[index]

		// Get "dependsOn"
		var dependsOn []interface{}
		dependsOnRaw, found := phase.Content.Get(`dependsOn`)
		if found {
			if v, ok := dependsOnRaw.([]interface{}); ok {
				dependsOn = v
			}
		}

		// Add to graph
		graph.AddNode(id)
		for _, depsId := range dependsOn {
			graph.AddEdge(id, cast.ToString(depsId))
		}
	}

	// Topological sort by dependencies
	order, cycles := graph.Sort()
	if len(cycles) > 0 {
		err := utils.NewMultiError()
		err.Append(fmt.Errorf(`found cycles in phases "dependsOn"`))
		for _, cycle := range cycles {
			var items []string
			for _, item := range cycle {
				items = append(items, item.(string))
			}
			err.AppendRaw(`  - ` + strings.Join(items, ` -> `))
		}
		errors.Append(err)
	}

	// Generate slice
	phases := make([]*model.Phase, 0)
	for index, id := range order {
		phase := l.phaseById[id.(string)]
		phase.PhaseKey = model.PhaseKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			Index:       index,
		}
		for taskIndex, task := range phase.Tasks {
			task.TaskKey = model.TaskKey{
				PhaseKey: phase.PhaseKey,
				Index:    taskIndex,
			}
			phase.Tasks[taskIndex] = task
		}
		phases = append(phases, phase)
	}

	// Fill in "dependsOn"
	for _, phase := range phases {
		// Get "dependsOn"
		var dependsOn []*model.Phase
		dependsOnRaw, found := phase.Content.Get(`dependsOn`)
		if found {
			if v, ok := dependsOnRaw.([]interface{}); ok {
				for _, id := range v {
					dependsOn = append(dependsOn, l.phaseById[cast.ToString(id)])
				}
			}

			// Remove "dependsOn" from content
			phase.Content.Delete(`dependsOn`)
		}

		// Sort dependsOn phases
		sort.SliceStable(dependsOn, func(i, j int) bool {
			return dependsOn[i].Index < dependsOn[j].Index
		})

		// Convert ID -> PhaseKey (index)
		phase.DependsOn = make([]model.PhaseKey, 0)
		for _, depPhase := range dependsOn {
			phase.DependsOn = append(phase.DependsOn, depPhase.PhaseKey)
		}
	}

	return phases, errors.ErrorOrNil()
}
