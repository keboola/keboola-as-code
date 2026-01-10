package orchestrator

import (
	"context"
	"strings"

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

	return nil
}

// buildPipelineYAML constructs the PipelineYAML structure from the orchestration.
// Note: Config metadata (name, description, disabled, _keboola) is stored in _config.yml
// by the corefiles mapper, so we don't duplicate it here.
func (w *localWriter) buildPipelineYAML() *model.PipelineYAML {
	pipeline := &model.PipelineYAML{
		Version: 2,
		Phases:  make([]model.PhaseYAML, 0),
	}

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

	// Set config path and full path
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
			// Path is full path from project root (without ./ prefix)
			taskYAML.Path = normalizeConfigPath(targetConfig.Path())
		}
	} else if task.ConfigData != nil {
		// For inline config, serialize the config data
		taskYAML.Parameters = orderedMapToMap(task.ConfigData)
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
	if pipeline.Name != "" {
		l.config.Name = pipeline.Name
	}
	if pipeline.Description != "" {
		l.config.Description = pipeline.Description
	}
	l.config.IsDisabled = pipeline.Disabled

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

	// Handle inline parameters
	if taskYAML.Parameters != nil {
		task.ConfigData = mapToOrderedMap(taskYAML.Parameters)
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

// Helper to convert config path to relative path for display
func normalizeConfigPath(configPath string) string {
	return strings.TrimPrefix(configPath, "./")
}
