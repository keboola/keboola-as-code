package orchestrator

import (
	"fmt"
	"sort"
	"strconv"
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

	errors := utils.NewMultiError()
	config := recipe.InternalObject.(*model.Config)

	// Get phases
	phasesRaw, found := config.Content.Get(model.OrchestratorPhasesContentKey)
	if !found {
		return nil
	}
	phases, ok := phasesRaw.([]interface{})
	if !ok {
		errors.Append(fmt.Errorf(`missing "%s" key in %s`, model.OrchestratorPhasesContentKey, config.Desc()))
	}

	// Get tasks
	tasksRaw, found := config.Content.Get(model.OrchestratorTasksContentKey)
	if !found {
		return nil
	}
	tasks, ok := tasksRaw.([]interface{})
	if !ok {
		errors.Append(fmt.Errorf(`missing "%s" key in %s`, model.OrchestratorTasksContentKey, config.Desc()))
	}

	// Can parsing continue?
	if errors.Len() > 0 {
		m.Logger.Warn(`Warning: `, utils.PrefixError(fmt.Sprintf(`invalid orchestrator %s`, config.Desc()), errors))
		return nil
	}

	// Delete keys
	config.Content.Delete(model.OrchestratorPhasesContentKey)
	config.Content.Delete(model.OrchestratorTasksContentKey)

	// Create ID -> phase map
	phaseById, err := m.parsePhasesFromRemote(phases)
	if err != nil {
		errors.Append(err)
	}

	// Sort phases by dependencies
	sortedPhases, err := m.sortPhases(phaseById, config.ConfigKey)
	if err != nil {
		errors.Append(err)
	}

	// Parse tasks
	if err := m.parseTasksFromRemote(phaseById, tasks); err != nil {
		errors.Append(err)
	}

	// Convert pointers to values
	config.Orchestration = &model.Orchestration{}
	for _, phase := range sortedPhases {
		config.Orchestration.Phases = append(config.Orchestration.Phases, *phase)
	}

	// Convert errors to warning
	if errors.Len() > 0 {
		m.Logger.Warn(`Warning: `, utils.PrefixError(fmt.Sprintf(`invalid orchestrator %s`, config.Desc()), errors))
	}

	return nil
}

func (m *orchestratorMapper) parsePhasesFromRemote(phases []interface{}) (map[string]*model.Phase, error) {
	errors := utils.NewMultiError()
	phaseById := make(map[string]*model.Phase)
	for index, phaseRaw := range phases {
		phaseContent, ok := phaseRaw.(orderedmap.OrderedMap)
		if !ok {
			continue
		}

		// Get ID
		phaseIdRaw, found := phaseContent.Get(`id`)
		if !found {
			errors.Append(fmt.Errorf(`missing phase[%d] "id" key`, index))
			continue
		}
		phaseId, err := strconv.Atoi(cast.ToString(phaseIdRaw))
		if err != nil {
			errors.Append(fmt.Errorf(`phase[%d] "id" must be int, found %T`, index, phaseIdRaw))
			continue
		}
		phaseContent.Delete(`id`)

		// Get name
		nameRaw, found := phaseContent.Get(`name`)
		if !found {
			errors.Append(fmt.Errorf(`missing phase[%d] "name" key`, index))
			continue
		}
		name, ok := nameRaw.(string)
		if !ok {
			errors.Append(fmt.Errorf(`phase[%d] "name" must be string, found %T`, index, nameRaw))
			continue
		}
		phaseContent.Delete(`name`)

		// Add to map
		phaseById[cast.ToString(phaseId)] = &model.Phase{
			Name:    name,
			Content: &phaseContent,
		}
	}
	return phaseById, errors.ErrorOrNil()
}

func (m *orchestratorMapper) parseTasksFromRemote(phaseById map[string]*model.Phase, tasks []interface{}) error {
	errors := utils.NewMultiError()
	for index, taskRaw := range tasks {
		taskContent, ok := taskRaw.(orderedmap.OrderedMap)
		if !ok {
			continue
		}

		// Get ID
		taskIdRaw, found := taskContent.Get(`id`)
		if !found {
			errors.Append(fmt.Errorf(`missing task[%d] "id" key`, index))
			continue
		}
		_, err := strconv.Atoi(cast.ToString(taskIdRaw))
		if err != nil {
			errors.Append(fmt.Errorf(`task[%d] "id" must be int, found %T`, index, taskIdRaw))
			continue
		}
		taskContent.Delete(`id`)

		// Get name
		nameRaw, found := taskContent.Get(`name`)
		if !found {
			errors.Append(fmt.Errorf(`missing task[%d] "name" key`, index))
			continue
		}
		name, ok := nameRaw.(string)
		if !ok {
			errors.Append(fmt.Errorf(`task[%d] "name" must be string, found %T`, index, nameRaw))
			continue
		}
		taskContent.Delete(`name`)

		// Get phase
		phaseIdRaw, found := taskContent.Get(`phase`)
		if !found {
			errors.Append(fmt.Errorf(`missing "phase" key in task[%d] "%s"`, index, name))
			continue
		}
		phaseId, err := strconv.Atoi(cast.ToString(phaseIdRaw))
		if err != nil {
			errors.Append(fmt.Errorf(`task "name" key must be string, found %T, in task[%d] "%s"`, phaseIdRaw, index, name))
			continue
		}
		phase, found := phaseById[cast.ToString(phaseId)]
		if !found {
			errors.Append(fmt.Errorf(`phase "%d" not found, referenced from task[%d] "%s"`, phaseId, index, name))
			continue
		}
		taskContent.Delete(`phase`)

		// Get target config
		targetRaw, found := taskContent.Get(`task`)
		if !found {
			errors.Append(fmt.Errorf(`missing "task" key in task[%d] "%s"`, index, name))
			continue
		}
		target, ok := targetRaw.(orderedmap.OrderedMap)
		if !ok {
			errors.Append(fmt.Errorf(`"task" key must be object, found %T, in task[%d] "%s"`, targetRaw, index, name))
			continue
		}

		// Get componentId
		componentIdRaw, found := target.Get(`componentId`)
		if !found {
			errors.Append(fmt.Errorf(`missing "task.componentId" key in task[%d] "%s"`, index, name))
			continue
		}
		componentId, ok := componentIdRaw.(string)
		if !ok {
			errors.Append(fmt.Errorf(`"task.componentId" must be string, found %T, in task[%d] "%s"`, componentIdRaw, index, name))
			continue
		}
		target.Delete(`componentId`)
		taskContent.Set(`task`, target)

		// Get configId
		configIdRaw, found := target.Get(`configId`)
		if !found {
			errors.Append(fmt.Errorf(`missing "task.configId" key in task[%d] "%s"`, index, name))
			continue
		}
		configId, ok := configIdRaw.(string)
		if !ok {
			errors.Append(fmt.Errorf(`"task.configId" key must be string, found %T, in task[%d] "%s"`, configIdRaw, index, name))
			continue
		}
		target.Delete(`configId`)
		taskContent.Set(`task`, target)

		// Add to map
		task := model.Task{
			TaskKey: model.TaskKey{
				PhaseKey: phase.PhaseKey,
				Index:    len(phase.Tasks),
			},
			Name:        name,
			Content:     &taskContent,
			ComponentId: componentId,
			ConfigId:    configId,
		}
		phase.Tasks = append(phase.Tasks, task)
	}
	return errors.ErrorOrNil()
}

func (m *orchestratorMapper) sortPhases(phaseById map[string]*model.Phase, configKey model.ConfigKey) ([]*model.Phase, error) {
	errors := utils.NewMultiError()
	graph := &toposort.Sorter{}

	// Sort keys, so sort results will be always same.
	// Some phases can have same deps, but their order must not be random.
	var keys []string
	for key := range phaseById {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Add dependencies to graph
	for _, id := range keys {
		phase := phaseById[id]

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
		phase := phaseById[id.(string)]
		phase.PhaseKey = model.PhaseKey{
			BranchId:    configKey.BranchId,
			ComponentId: configKey.ComponentId,
			ConfigId:    configKey.Id,
			Index:       index,
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
					dependsOn = append(dependsOn, phaseById[cast.ToString(id)])
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
