package corefiles

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad loads files to tagged object (Branch, Config,ConfigRow) fields.
func (m *coreFilesMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	errs := errors.NewMultiError()

	// Check if unified _config.yml exists for Config/ConfigRow objects
	configYAMLPath := m.state.NamingGenerator().ConfigYAMLFilePath(recipe.ObjectManifest.Path())
	if m.state.ObjectsRoot().IsFile(ctx, configYAMLPath) {
		// Load from unified _config.yml
		if config, ok := recipe.Object.(*model.Config); ok {
			if err := m.loadUnifiedConfigYAML(ctx, recipe, config); err != nil {
				errs.Append(err)
			}
			return errs.ErrorOrNil()
		}
		if configRow, ok := recipe.Object.(*model.ConfigRow); ok {
			if err := m.loadUnifiedConfigRowYAML(ctx, recipe, configRow); err != nil {
				errs.Append(err)
			}
			return errs.ErrorOrNil()
		}
	}

	// Fall back to legacy format (meta.json, config.json, description.md)
	if err := m.loadMetaFile(ctx, recipe); err != nil {
		errs.Append(err)
	}
	if err := m.loadConfigFile(ctx, recipe); err != nil {
		errs.Append(err)
	}
	if err := m.loadDescriptionFile(ctx, recipe); err != nil {
		errs.Append(err)
	}
	return errs.ErrorOrNil()
}

// loadMetaFile from meta.json.
func (m *coreFilesMapper) loadMetaFile(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().MetaFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name+" metadata").
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectMeta).
		ReadJSONFieldsTo(ctx, recipe.Object, model.MetaFileFieldsTag)
	return err
}

// loadConfigFile from config.json.
func (m *coreFilesMapper) loadConfigFile(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().ConfigFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectConfig).
		ReadJSONMapTo(ctx, recipe.Object, model.ConfigFileFieldTag)
	return err
}

// loadDescriptionFile from description.md.
func (m *coreFilesMapper) loadDescriptionFile(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().DescriptionFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name+" description").
		AddTag(model.FileTypeMarkdown).
		AddTag(model.FileKindObjectDescription).
		ReadFileContentTo(ctx, recipe.Object, model.DescriptionFileFieldTag)
	return err
}

// loadUnifiedConfigYAML loads configuration from _config.yml for Config objects.
func (m *coreFilesMapper) loadUnifiedConfigYAML(ctx context.Context, recipe *model.LocalLoadRecipe, config *model.Config) error {
	configYAMLPath := m.state.NamingGenerator().ConfigYAMLFilePath(recipe.ObjectManifest.Path())

	// Read the file
	file, err := recipe.Files.
		Load(configYAMLPath).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription("unified configuration").
		AddTag(model.FileTypeYaml).
		AddTag(model.FileKindObjectConfig).
		ReadFile(ctx)
	if err != nil {
		return err
	}

	// Parse YAML
	var configYAML model.ConfigYAML
	if err := yaml.Unmarshal([]byte(file.Content), &configYAML); err != nil {
		return err
	}

	// Apply metadata to config
	config.Name = configYAML.Name
	config.Description = configYAML.Description
	config.IsDisabled = configYAML.Disabled

	// Check if this is a scheduler component - restore full content directly
	component, compErr := m.state.Components().GetOrErr(config.ComponentID)
	if compErr == nil && component.IsScheduler() {
		// For scheduler configs, Parameters contains the full content (schedule, target, etc.)
		// Restore it directly without wrapping in "parameters"
		if configYAML.Parameters != nil {
			config.Content = mapToOrderedMap(configYAML.Parameters)
		} else {
			config.Content = orderedmap.New()
		}
		return nil
	}

	// Check if this is an orchestrator - build Orchestration from phases
	if compErr == nil && component.IsOrchestrator() {
		config.Content = orderedmap.New() // Initialize empty content, remote_save will rebuild it
		config.Orchestration = m.buildOrchestrationFromConfigYAML(recipe, config, &configYAML)
		// Update scheduler configs with inline schedule data (orchestrator's _config.yml is the source of truth)
		m.updateSchedulerConfigsFromInline(config, configYAML.Schedules)
		return nil
	}

	// Build Content (orderedmap) from ConfigYAML
	config.Content = buildContentFromConfigYAML(&configYAML)

	return nil
}

// loadUnifiedConfigRowYAML loads configuration from _config.yml for ConfigRow objects.
func (m *coreFilesMapper) loadUnifiedConfigRowYAML(ctx context.Context, recipe *model.LocalLoadRecipe, configRow *model.ConfigRow) error {
	configYAMLPath := m.state.NamingGenerator().ConfigYAMLFilePath(recipe.ObjectManifest.Path())

	// Read the file
	file, err := recipe.Files.
		Load(configYAMLPath).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription("unified configuration").
		AddTag(model.FileTypeYaml).
		AddTag(model.FileKindObjectConfig).
		ReadFile(ctx)
	if err != nil {
		return err
	}

	// Parse YAML
	var configYAML model.ConfigYAML
	if err := yaml.Unmarshal([]byte(file.Content), &configYAML); err != nil {
		return err
	}

	// Apply metadata to configRow
	configRow.Name = configYAML.Name
	configRow.Description = configYAML.Description
	configRow.IsDisabled = configYAML.Disabled

	// Build Content (orderedmap) from ConfigYAML
	configRow.Content = buildContentFromConfigYAML(&configYAML)

	return nil
}

// buildContentFromConfigYAML converts ConfigYAML back to orderedmap Content.
func buildContentFromConfigYAML(configYAML *model.ConfigYAML) *orderedmap.OrderedMap {
	content := orderedmap.New()

	// Build storage section
	storage := orderedmap.New()
	hasStorage := false

	// Build input tables
	if configYAML.Input != nil && len(configYAML.Input.Tables) > 0 {
		input := orderedmap.New()
		tables := make([]any, 0)
		for _, t := range configYAML.Input.Tables {
			table := orderedmap.New()
			table.Set("source", t.Source)
			table.Set("destination", t.Destination)
			if len(t.Columns) > 0 {
				cols := make([]any, len(t.Columns))
				for i, c := range t.Columns {
					cols[i] = c
				}
				table.Set("columns", cols)
			}
			if t.WhereColumn != "" {
				table.Set("where_column", t.WhereColumn)
			}
			if t.WhereOperator != "" {
				table.Set("where_operator", t.WhereOperator)
			}
			if len(t.WhereValues) > 0 {
				vals := make([]any, len(t.WhereValues))
				for i, v := range t.WhereValues {
					vals[i] = v
				}
				table.Set("where_values", vals)
			}
			if t.Limit > 0 {
				table.Set("limit", t.Limit)
			}
			tables = append(tables, table)
		}
		input.Set("tables", tables)
		storage.Set("input", input)
		hasStorage = true
	}

	// Build output tables
	if configYAML.Output != nil && len(configYAML.Output.Tables) > 0 {
		output := orderedmap.New()
		tables := make([]any, 0)
		for _, t := range configYAML.Output.Tables {
			table := orderedmap.New()
			table.Set("source", t.Source)
			table.Set("destination", t.Destination)
			if len(t.PrimaryKey) > 0 {
				pks := make([]any, len(t.PrimaryKey))
				for i, pk := range t.PrimaryKey {
					pks[i] = pk
				}
				table.Set("primary_key", pks)
			}
			if t.Incremental {
				table.Set("incremental", t.Incremental)
			}
			tables = append(tables, table)
		}
		output.Set("tables", tables)
		storage.Set("output", output)
		hasStorage = true
	}

	if hasStorage {
		content.Set("storage", storage)
	}

	// Add parameters
	if configYAML.Parameters != nil && len(configYAML.Parameters) > 0 {
		content.Set("parameters", mapToOrderedMap(configYAML.Parameters))
	}

	// Add runtime/backend
	if configYAML.Backend != nil && (configYAML.Backend.Type != "" || configYAML.Backend.Context != "") {
		runtime := orderedmap.New()
		backend := orderedmap.New()
		if configYAML.Backend.Type != "" {
			backend.Set("type", configYAML.Backend.Type)
		}
		if configYAML.Backend.Context != "" {
			backend.Set("context", configYAML.Backend.Context)
		}
		runtime.Set("backend", backend)
		content.Set("runtime", runtime)
	}

	return content
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

// buildOrchestrationFromConfigYAML converts ConfigYAML phases to model.Orchestration.
func (m *coreFilesMapper) buildOrchestrationFromConfigYAML(recipe *model.LocalLoadRecipe, config *model.Config, configYAML *model.ConfigYAML) *model.Orchestration {
	orchestration := &model.Orchestration{
		Phases: make([]*model.Phase, 0),
	}

	// Create a map of phase names to indices for resolving dependencies
	phaseNameToIndex := make(map[string]int)
	for i, phaseYAML := range configYAML.Phases {
		phaseNameToIndex[phaseYAML.Name] = i
	}

	for phaseIndex, phaseYAML := range configYAML.Phases {
		phase := &model.Phase{
			PhaseKey: model.PhaseKey{
				BranchID:    config.BranchID,
				ComponentID: config.ComponentID,
				ConfigID:    config.ID,
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
					BranchID:    config.BranchID,
					ComponentID: config.ComponentID,
					ConfigID:    config.ID,
					Index:       depIndex,
				})
			}
		}

		// Build tasks
		for taskIndex, taskYAML := range phaseYAML.Tasks {
			task := m.buildTaskFromConfigYAML(recipe, config, &taskYAML, phaseIndex, taskIndex)
			phase.Tasks = append(phase.Tasks, task)
		}

		orchestration.Phases = append(orchestration.Phases, phase)
	}

	return orchestration
}

// buildTaskFromConfigYAML converts TaskYAML to model.Task.
func (m *coreFilesMapper) buildTaskFromConfigYAML(recipe *model.LocalLoadRecipe, config *model.Config, taskYAML *model.TaskYAML, phaseIndex, taskIndex int) *model.Task {
	content := orderedmap.New()
	// Store continueOnFailure in content
	content.Set("continueOnFailure", taskYAML.ContinueOnFailure)

	task := &model.Task{
		TaskKey: model.TaskKey{
			PhaseKey: model.PhaseKey{
				BranchID:    config.BranchID,
				ComponentID: config.ComponentID,
				ConfigID:    config.ID,
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
		targetConfig, err := m.getTargetConfig(recipe, config, taskYAML.Config)
		if err != nil {
			// Log error but continue - task will have empty ConfigID
		} else if targetConfig != nil {
			task.ComponentID = targetConfig.ComponentID
			task.ConfigID = targetConfig.ID
			task.ConfigPath = m.state.MustGet(targetConfig.Key()).Path()
			markConfigUsedInOrchestrator(targetConfig, config)
		}
	}

	// Handle inline parameters
	if taskYAML.Parameters != nil {
		task.ConfigData = mapToOrderedMap(taskYAML.Parameters)
	}

	return task
}

// getTargetConfig resolves a config path to a Config object.
// The targetPath is relative from the branch directory.
func (m *coreFilesMapper) getTargetConfig(recipe *model.LocalLoadRecipe, config *model.Config, targetPath string) (*model.Config, error) {
	if len(targetPath) == 0 {
		return nil, nil
	}

	// Get the branch path as base for resolving relative paths
	branchKey := model.BranchKey{ID: config.BranchID}
	branchState, found := m.state.Get(branchKey)
	if !found {
		return nil, errors.Errorf(`branch "%d" not found`, config.BranchID)
	}

	// Target path is relative from branch directory
	fullTargetPath := filesystem.Join(branchState.Path(), targetPath)
	configStateRaw, found := m.state.GetByPath(fullTargetPath)
	if !found {
		return nil, errors.Errorf(`target config "%s" not found`, fullTargetPath)
	}

	configState, ok := configStateRaw.(*model.ConfigState)
	if !ok {
		return nil, errors.Errorf(`path "%s" must be config, found "%s"`, fullTargetPath, configStateRaw.Kind().String())
	}

	// During local load, other configs may not have their local state populated yet due to loading order.
	// Use LocalOrRemoteState() as a fallback - the ConfigID will be the same in both.
	targetConfig := configState.LocalOrRemoteState()
	if targetConfig == nil {
		return nil, errors.Errorf(`target config "%s" has no state`, fullTargetPath)
	}

	return targetConfig.(*model.Config), nil
}

// markConfigUsedInOrchestrator marks a config as used in an orchestrator.
func markConfigUsedInOrchestrator(targetConfig *model.Config, orchestratorConfig *model.Config) {
	targetConfig.Relations.Add(&model.UsedInOrchestratorRelation{
		ConfigID: orchestratorConfig.ID,
	})
}

// updateSchedulerConfigsFromInline stores inline schedules in the orchestration struct.
// These schedules will be used during push to create/update/delete scheduler configs via API.
func (m *coreFilesMapper) updateSchedulerConfigsFromInline(config *model.Config, schedules []model.ScheduleYAML) {
	if config.Orchestration == nil {
		return
	}
	config.Orchestration.Schedules = schedules
}
