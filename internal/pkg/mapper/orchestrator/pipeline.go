package orchestrator

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// savePipelineYAML saves orchestration as a single pipeline.yml file.
func (w *localWriter) savePipelineYAML(ctx context.Context) error {
	pipeline := w.buildPipelineYAML()

	// Encode to YAML
	content, err := yaml.Marshal(pipeline)
	if err != nil {
		return err
	}

	// Write the file
	pipelinePath := w.NamingGenerator().PipelineFilePath(w.Path())
	w.Files.
		Add(filesystem.NewRawFile(pipelinePath, string(content))).
		SetDescription("orchestration pipeline").
		AddTag(model.FileTypeYaml)

	// Delete old phases directory files
	w.deleteOldPhasesDir()

	// Note: Schedule directories are NOT deleted because scheduler configs are separate
	// API objects tracked by the manifest. The schedules are included inline in pipeline.yml
	// as a convenience, but the separate config directories must remain for manifest consistency.

	return nil
}

// buildPipelineYAML constructs the PipelineYAML structure from the orchestration.
// pipeline.yml contains all metadata for orchestrations (no separate _config.yml needed).
func (w *localWriter) buildPipelineYAML() *model.PipelineYAML {
	pipeline := &model.PipelineYAML{
		Version:     2,
		Name:        w.config.Name,
		Description: w.config.Description,
		Disabled:    w.config.IsDisabled,
		Phases:      make([]model.PhaseYAML, 0),
		Keboola: &model.KeboolaMetadata{
			ComponentID: w.config.ComponentID.String(),
			ConfigID:    w.config.ID.String(),
		},
	}

	// Add inline schedules
	pipeline.Schedules = w.buildSchedulesYAML()

	allPhases := w.config.Orchestration.Phases

	for _, phase := range allPhases {
		phaseYAML := model.PhaseYAML{
			Name:  phase.Name,
			Tasks: make([]model.TaskYAML, 0),
		}

		// Get description from Content
		if descRaw, ok := phase.Content.Get("description"); ok {
			if desc, ok := descRaw.(string); ok {
				phaseYAML.Description = desc
			}
		}

		// Build dependsOn list (using phase names)
		for _, depOnKey := range phase.DependsOn {
			depOnPhase := allPhases[depOnKey.Index]
			phaseYAML.DependsOn = append(phaseYAML.DependsOn, depOnPhase.Name)
		}

		// Build tasks
		for _, task := range phase.Tasks {
			taskYAML := w.buildTaskYAML(task)
			phaseYAML.Tasks = append(phaseYAML.Tasks, taskYAML)
		}

		pipeline.Phases = append(pipeline.Phases, phaseYAML)
	}

	return pipeline
}

// buildSchedulesYAML finds all scheduler configs related to this orchestrator and converts them to YAML format.
func (w *localWriter) buildSchedulesYAML() []model.ScheduleYAML {
	schedules := make([]model.ScheduleYAML, 0)

	// Find all scheduler configs that target this orchestrator
	for _, objectState := range w.All() {
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		// Check if this is a scheduler config
		config := configState.LocalOrRemoteState().(*model.Config)
		component, err := w.Components().GetOrErr(config.ComponentID)
		if err != nil || !component.IsScheduler() {
			continue
		}

		// Check if this scheduler targets our orchestrator
		for _, rel := range config.Relations.GetByType(model.SchedulerForRelType) {
			schedulerFor := rel.(*model.SchedulerForRelation)
			if schedulerFor.ComponentID == w.config.ComponentID && schedulerFor.ConfigID == w.config.ID {
				// Convert to YAML format
				scheduleYAML := w.buildScheduleYAML(config)
				schedules = append(schedules, scheduleYAML)
				break
			}
		}
	}

	return schedules
}

// buildScheduleYAML converts a scheduler config to ScheduleYAML.
func (w *localWriter) buildScheduleYAML(config *model.Config) model.ScheduleYAML {
	schedule := model.ScheduleYAML{
		Name: config.Name,
	}

	// Get description
	if config.Description != "" {
		schedule.Description = config.Description
	}

	// Extract schedule details from content
	if target, ok := config.Content.Get("target"); ok {
		if targetMap, ok := target.(*orderedmap.OrderedMap); ok {
			// Get mode (cron expression is in cronTab)
			if mode, ok := targetMap.Get("mode"); ok {
				if modeStr, ok := mode.(string); ok && modeStr == "run" {
					// This is a one-time run, not a scheduled one
					// For now, skip it
				}
			}
		}
	}

	// Get schedule (cron) details from the schedule key
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

	// Get variable values if present
	if varsRaw, ok := config.Content.Get("variableValuesId"); ok {
		if varsID, ok := varsRaw.(string); ok && varsID != "" {
			// Variable values are stored separately, we'd need to look them up
			// For now, we'll leave variables empty
			_ = varsID
		}
	}

	return schedule
}

// buildTaskYAML constructs a TaskYAML from a model.Task.
func (w *localWriter) buildTaskYAML(task *model.Task) model.TaskYAML {
	taskYAML := model.TaskYAML{
		Name:      task.Name,
		Component: task.ComponentID.String(),
	}

	// Get continueOnFailure from Content
	if task.Content != nil {
		if continueOnFailureRaw, ok := task.Content.Get("continueOnFailure"); ok {
			if continueOnFailure, ok := continueOnFailureRaw.(bool); ok {
				taskYAML.ContinueOnFailure = continueOnFailure
			}
		}
	}

	// Handle enabled field (only set if false)
	if !task.Enabled {
		enabled := false
		taskYAML.Enabled = &enabled
	}

	// Set config path (relative from orchestrator directory)
	if len(task.ConfigID) > 0 {
		// Get target config path
		targetKey := &model.ConfigKey{
			BranchID:    task.BranchID,
			ComponentID: task.ComponentID,
			ID:          task.ConfigID,
		}

		if targetConfig, found := w.Get(targetKey); found {
			// Config is relative path from orchestrator directory
			targetPath, _ := filesystem.Rel(w.configPath.GetParentPath(), targetConfig.Path())
			taskYAML.Config = targetPath
		}
	} else if task.ConfigData != nil {
		// For inline config, serialize the config data - preserve ordering
		taskYAML.Parameters = task.ConfigData.Clone()
	}

	return taskYAML
}

// orderedMapToMap converts an orderedmap to a regular map.
func orderedMapToMap(om *orderedmap.OrderedMap) map[string]any {
	if om == nil {
		return nil
	}

	result := make(map[string]any)
	for _, key := range om.Keys() {
		value, _ := om.Get(key)
		switch v := value.(type) {
		case *orderedmap.OrderedMap:
			result[key] = orderedMapToMap(v)
		default:
			result[key] = v
		}
	}
	return result
}

// deleteOldPhasesDir marks old phases directory files for deletion.
func (w *localWriter) deleteOldPhasesDir() {
	phasesDir := w.NamingGenerator().PhasesDir(w.Path())

	for _, path := range w.TrackedPaths() {
		if filesystem.IsFrom(path, phasesDir) && w.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}
}

// deleteSchedulesDirs marks schedule config directories for deletion since they're now inline in pipeline.yml.
func (w *localWriter) deleteSchedulesDirs() {
	schedulesDir := filesystem.Join(w.Path(), "schedules")

	for _, path := range w.TrackedPaths() {
		if filesystem.IsFrom(path, schedulesDir) && w.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}
}

// loadPipelineYAML loads orchestration from a pipeline.yml file.
func (l *localLoader) loadPipelineYAML(ctx context.Context, pipelinePath string) error {
	// Track the pipeline file
	l.manifest.AddRelatedPath(pipelinePath)

	// Read the file
	file, err := l.files.
		Load(pipelinePath).
		AddMetadata(filesystem.ObjectKeyMetadata, l.config.Key()).
		SetDescription("orchestration pipeline").
		AddTag(model.FileTypeYaml).
		ReadFile(ctx)
	if err != nil {
		return err
	}

	// Parse YAML
	var pipeline model.PipelineYAML
	if err := yaml.Unmarshal([]byte(file.Content), &pipeline); err != nil {
		return err
	}

	// Apply metadata from pipeline.yml to config
	// pipeline.yml is the sole source of metadata for orchestrations (no _config.yml)
	l.config.Name = pipeline.Name
	l.config.Description = pipeline.Description
	l.config.IsDisabled = pipeline.Disabled

	// Initialize Content (required for validation)
	// The remote_save mapper will rebuild it from Orchestration when pushing to API
	l.config.Content = orderedmap.New()

	// Convert to model.Orchestration
	l.config.Orchestration = l.buildOrchestrationFromPipeline(&pipeline)

	return nil
}

// buildOrchestrationFromPipeline converts PipelineYAML to model.Orchestration.
func (l *localLoader) buildOrchestrationFromPipeline(pipeline *model.PipelineYAML) *model.Orchestration {
	orchestration := &model.Orchestration{
		Phases: make([]*model.Phase, 0),
	}

	// Create a map of phase names to indices for resolving dependencies
	phaseNameToIndex := make(map[string]int)
	for i, phaseYAML := range pipeline.Phases {
		phaseNameToIndex[phaseYAML.Name] = i
	}

	for phaseIndex, phaseYAML := range pipeline.Phases {
		phase := &model.Phase{
			PhaseKey: model.PhaseKey{
				BranchID:    l.config.BranchID,
				ComponentID: l.config.ComponentID,
				ConfigID:    l.config.ID,
				Index:       phaseIndex,
			},
			Name:    phaseYAML.Name,
			Content: orderedmap.New(),
			Tasks:   make([]*model.Task, 0),
		}

		// Set description in Content
		if phaseYAML.Description != "" {
			phase.Content.Set("description", phaseYAML.Description)
		}

		// Resolve dependsOn (convert names to PhaseKeys)
		for _, depName := range phaseYAML.DependsOn {
			if depIndex, ok := phaseNameToIndex[depName]; ok {
				phase.DependsOn = append(phase.DependsOn, model.PhaseKey{
					BranchID:    l.config.BranchID,
					ComponentID: l.config.ComponentID,
					ConfigID:    l.config.ID,
					Index:       depIndex,
				})
			}
		}

		// Build tasks
		for taskIndex, taskYAML := range phaseYAML.Tasks {
			task := l.buildTaskFromYAML(&taskYAML, phaseIndex, taskIndex)
			phase.Tasks = append(phase.Tasks, task)
		}

		orchestration.Phases = append(orchestration.Phases, phase)
	}

	return orchestration
}

// buildTaskFromYAML converts TaskYAML to model.Task.
func (l *localLoader) buildTaskFromYAML(taskYAML *model.TaskYAML, phaseIndex, taskIndex int) *model.Task {
	content := orderedmap.New()
	// Store continueOnFailure in content
	content.Set("continueOnFailure", taskYAML.ContinueOnFailure)

	task := &model.Task{
		TaskKey: model.TaskKey{
			PhaseKey: model.PhaseKey{
				BranchID:    l.config.BranchID,
				ComponentID: l.config.ComponentID,
				ConfigID:    l.config.ID,
				Index:       phaseIndex,
			},
			Index: taskIndex,
		},
		Name:        taskYAML.Name,
		ComponentID: keboola.ComponentID(taskYAML.Component),
		Enabled:     true, // Default to enabled
		Content:     content,
	}

	// Handle enabled field
	if taskYAML.Enabled != nil {
		task.Enabled = *taskYAML.Enabled
	}

	// Resolve config path to ConfigID
	if taskYAML.Config != "" {
		targetConfig, err := l.getTargetConfig(taskYAML.Config)
		if err != nil {
			l.errors.Append(err)
		} else if targetConfig != nil {
			task.ComponentID = targetConfig.ComponentID
			task.ConfigID = targetConfig.ID
			task.ConfigPath = l.MustGet(targetConfig.Key()).Path()
			markConfigUsedInOrchestrator(targetConfig, l.config)
		}
	}

	// Handle inline parameters - use orderedmap directly to preserve ordering
	if taskYAML.Parameters != nil {
		task.ConfigData = taskYAML.Parameters.Clone()
	}

	return task
}

// mapToOrderedMap converts a regular map to an orderedmap.
func mapToOrderedMap(m map[string]any) *orderedmap.OrderedMap {
	if m == nil {
		return nil
	}

	result := orderedmap.New()
	for key, value := range m {
		switch v := value.(type) {
		case map[string]any:
			result.Set(key, mapToOrderedMap(v))
		default:
			result.Set(key, v)
		}
	}
	return result
}

// hasPipelineYAML checks if a pipeline.yml file exists.
func (l *localLoader) hasPipelineYAML(ctx context.Context) bool {
	pipelinePath := l.NamingGenerator().PipelineFilePath(l.Path())
	return l.ObjectsRoot().IsFile(ctx, pipelinePath)
}
