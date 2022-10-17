package orchestrator

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *orchestratorMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestratorConfigKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}

	object := recipe.Object.(*model.Config)
	m.serializeOrchestrationTo(object, object.Orchestration)

	if recipe.ChangedFields.Has(`orchestration`) {
		// Orchestration is stored in configuration in the API
		recipe.ChangedFields.Add(`configuration`)
		recipe.ChangedFields.Remove(`orchestration`)
	}

	return nil
}

func (m *orchestratorMapper) serializeOrchestrationTo(config *model.Config, orchestration *model.Orchestration) {
	phases := make([]interface{}, 0)
	tasks := make([]interface{}, 0)

	// Map structs
	taskId := 0
	for index, phase := range orchestration.Phases {
		phaseId := index + 1
		phaseContent := orderedmap.New()
		phaseContent.Set(`id`, phaseId)
		phaseContent.Set(`name`, phase.Name)

		// Map dependsOn
		dependsOn := make([]int, 0)
		for _, depOnPhase := range phase.DependsOn {
			dependsOn = append(dependsOn, depOnPhase.Index+1)
		}
		phaseContent.Set(`dependsOn`, dependsOn)

		// Copy additional content
		for _, k := range phase.Content.Keys() {
			v, _ := phase.Content.Get(k)
			phaseContent.Set(k, v)
		}

		// Map tasks
		for _, task := range phase.Tasks {
			taskId++
			taskContent := orderedmap.New()
			taskContent.Set(`id`, taskId)
			taskContent.Set(`name`, task.Name)
			taskContent.Set(`enabled`, task.Enabled)
			taskContent.Set(`phase`, phaseId)

			// Copy additional content
			for _, k := range task.Content.Keys() {
				v, _ := task.Content.Get(k)
				taskContent.Set(k, v)
			}

			// Get "task" value
			var target *orderedmap.OrderedMap
			taskMapRaw, found := task.Content.Get(`task`)
			if found {
				if v, ok := taskMapRaw.(*orderedmap.OrderedMap); ok {
					target = v
				}
			}
			if target == nil {
				target = orderedmap.New()
			}

			// Set componentId/configId
			if len(task.ComponentId) > 0 {
				target.Set(`componentId`, task.ComponentId.String())
			}
			if len(task.ConfigId) > 0 {
				target.Set(`configId`, task.ConfigId.String())
			} else if task.ConfigData != nil {
				target.Set(`configData`, task.ConfigData)
			}
			taskContent.Set(`task`, *target)

			// Add to output
			tasks = append(tasks, *taskContent)
		}

		// Add to output
		phases = append(phases, *phaseContent)
	}

	// Update config
	config.Orchestration = nil
	config.Content.Set(model.OrchestratorPhasesContentKey, phases)
	config.Content.Set(model.OrchestratorTasksContentKey, tasks)
}
