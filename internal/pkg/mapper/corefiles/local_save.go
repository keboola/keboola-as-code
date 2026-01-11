package corefiles

import (
	"context"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
)

const (
	HideMetaFileFieldsAnnotation = `hideMetaFileFields`
)

// MapBeforeLocalSave saves tagged object (Branch, Config,ConfigRow) fields to a files.
func (m *coreFilesMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Use unified _config.yml for Config objects
	if config, ok := recipe.Object.(*model.Config); ok {
		// Skip file generation for scheduler configs targeting orchestrators
		// (these are represented inline in the orchestrator's _config.yml)
		if m.isSchedulerForOrchestrator(config) {
			return nil
		}
		return m.createUnifiedConfigYAML(recipe, config)
	}

	// Use unified _config.yml for ConfigRow objects
	if configRow, ok := recipe.Object.(*model.ConfigRow); ok {
		return m.createUnifiedConfigRowYAML(recipe, configRow)
	}

	// For other objects (Branch, etc.), use the traditional format
	m.createMetaFile(recipe)
	m.createConfigFile(recipe)
	m.createDescriptionFile(recipe)
	return nil
}

// createMetaFile meta.json.
func (m *coreFilesMapper) createMetaFile(recipe *model.LocalSaveRecipe) {
	if metadata := reflecthelper.MapFromTaggedFields(model.MetaFileFieldsTag, recipe.Object); metadata != nil {
		path := m.state.NamingGenerator().MetaFilePath(recipe.Path())
		jsonFile := filesystem.NewJSONFile(path, metadata)

		// Remove hidden fields, the annotation can be set by some other mapper.
		if hiddenFields, ok := recipe.Annotations[HideMetaFileFieldsAnnotation].([]string); ok {
			for _, field := range hiddenFields {
				jsonFile.Content.Delete(field)
			}
		}

		recipe.Files.
			Add(jsonFile).
			AddTag(model.FileTypeJSON).
			AddTag(model.FileKindObjectMeta)
	}
}

// createConfigFile config.json.
func (m *coreFilesMapper) createConfigFile(recipe *model.LocalSaveRecipe) {
	if configuration := reflecthelper.MapFromOneTaggedField(model.ConfigFileFieldTag, recipe.Object); configuration != nil {
		path := m.state.NamingGenerator().ConfigFilePath(recipe.Path())
		jsonFile := filesystem.NewJSONFile(path, configuration)
		recipe.Files.
			Add(jsonFile).
			AddTag(model.FileTypeJSON).
			AddTag(model.FileKindObjectConfig)
	}
}

// createDescriptionFile description.md.
func (m *coreFilesMapper) createDescriptionFile(recipe *model.LocalSaveRecipe) {
	if description, found := reflecthelper.StringFromOneTaggedField(model.DescriptionFileFieldTag, recipe.Object); found {
		path := m.state.NamingGenerator().DescriptionFilePath(recipe.Path())
		markdownFile := filesystem.NewRawFile(path, strings.TrimRight(description, " \r\n\t")+"\n")
		recipe.Files.
			Add(markdownFile).
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindObjectDescription)
	}
}

// createUnifiedConfigYAML creates a single _config.yml file that combines
// metadata (meta.json), description (description.md), and configuration (config.json).
func (m *coreFilesMapper) createUnifiedConfigYAML(recipe *model.LocalSaveRecipe, config *model.Config) error {
	// Build the unified ConfigYAML structure
	configYAML := m.buildConfigYAML(config)

	// Marshal to YAML
	content, err := yaml.Marshal(configYAML)
	if err != nil {
		return err
	}

	// Create the _config.yml file
	path := m.state.NamingGenerator().ConfigYAMLFilePath(recipe.Path())
	recipe.Files.
		Add(filesystem.NewRawFile(path, string(content))).
		SetDescription("unified configuration").
		AddTag(model.FileTypeYaml).
		AddTag(model.FileKindObjectConfig)

	// Mark old files for deletion
	m.deleteOldCoreFiles(recipe)

	return nil
}

// createUnifiedConfigRowYAML creates a single _config.yml file for ConfigRow.
func (m *coreFilesMapper) createUnifiedConfigRowYAML(recipe *model.LocalSaveRecipe, configRow *model.ConfigRow) error {
	// Build the unified ConfigYAML structure for row
	configYAML := m.buildConfigRowYAML(configRow)

	// Marshal to YAML
	content, err := yaml.Marshal(configYAML)
	if err != nil {
		return err
	}

	// Create the _config.yml file
	path := m.state.NamingGenerator().ConfigYAMLFilePath(recipe.Path())
	recipe.Files.
		Add(filesystem.NewRawFile(path, string(content))).
		SetDescription("unified configuration").
		AddTag(model.FileTypeYaml).
		AddTag(model.FileKindObjectConfig)

	// Mark old files for deletion
	m.deleteOldCoreFiles(recipe)

	return nil
}

// buildConfigYAML constructs the ConfigYAML structure from a Config object.
func (m *coreFilesMapper) buildConfigYAML(config *model.Config) *model.ConfigYAML {
	configYAML := &model.ConfigYAML{
		Version:     2,
		Name:        config.Name,
		Description: strings.TrimRight(config.Description, " \r\n\t"),
		Disabled:    config.IsDisabled,
		Keboola: &model.KeboolaMetaYAML{
			ComponentID: config.ComponentID.String(),
			ConfigID:    config.ID.String(),
		},
	}

	// Check if this is a scheduler component - preserve full content
	component, err := m.state.Components().GetOrErr(config.ComponentID)
	if err == nil && component.IsScheduler() {
		// For scheduler configs, preserve the entire content as-is
		if config.Content != nil {
			configYAML.Parameters = config.Content.Clone()
		}
		return configYAML
	}

	// Check if this is an orchestrator/flow - build phases and schedules
	if err == nil && orchestrator.IsOrchestratorOrFlow(component) {
		m.buildOrchestratorConfigYAML(config, configYAML)
		return configYAML
	}

	// Extract configuration from Content (orderedmap)
	if config.Content != nil {
		// Extract storage input/output
		if storage, ok := config.Content.Get("storage"); ok {
			if storageMap, ok := storage.(*orderedmap.OrderedMap); ok {
				configYAML.Input = extractStorageInput(storageMap)
				configYAML.Output = extractStorageOutput(storageMap)
			}
		}

		// Extract parameters - preserve ordering by using orderedmap directly
		if params, ok := config.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(*orderedmap.OrderedMap); ok {
				configYAML.Parameters = paramsMap.Clone()
			} else if paramsMapAny, ok := params.(map[string]any); ok {
				// Convert regular map to orderedmap (will sort alphabetically, but at least be consistent)
				configYAML.Parameters = orderedmap.FromPairs(mapToPairs(paramsMapAny))
			}
		}

		// Extract runtime fields (backend, safe, etc.) as a unified object - preserve ordering
		if runtime, ok := config.Content.Get("runtime"); ok {
			if runtimeMap, ok := runtime.(*orderedmap.OrderedMap); ok {
				configYAML.Runtime = runtimeMap.Clone()
			}
		}
	}

	return configYAML
}

// buildOrchestratorConfigYAML adds orchestration-specific fields (phases, schedules) to ConfigYAML.
func (m *coreFilesMapper) buildOrchestratorConfigYAML(config *model.Config, configYAML *model.ConfigYAML) {
	if config.Orchestration == nil {
		return
	}

	// Build phases
	allPhases := config.Orchestration.Phases
	for _, phase := range allPhases {
		phaseYAML := model.PhaseYAML{
			Name:  phase.Name,
			Tasks: make([]model.TaskYAML, 0),
		}

		// Get description from Content
		if phase.Content != nil {
			if descRaw, ok := phase.Content.Get("description"); ok {
				if desc, ok := descRaw.(string); ok {
					phaseYAML.Description = desc
				}
			}
		}

		// Build dependsOn list (using phase names)
		for _, depOnKey := range phase.DependsOn {
			depOnPhase := allPhases[depOnKey.Index]
			phaseYAML.DependsOn = append(phaseYAML.DependsOn, depOnPhase.Name)
		}

		// Build tasks
		for _, task := range phase.Tasks {
			taskYAML := m.buildTaskYAML(config, task)
			phaseYAML.Tasks = append(phaseYAML.Tasks, taskYAML)
		}

		configYAML.Phases = append(configYAML.Phases, phaseYAML)
	}

	// Build schedules from related scheduler configs
	configYAML.Schedules = m.buildSchedulesYAML(config)
}

// buildTaskYAML constructs a TaskYAML from a model.Task.
func (m *coreFilesMapper) buildTaskYAML(config *model.Config, task *model.Task) model.TaskYAML {
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
		// Use the orchestrator's BranchID since target configs are in the same branch
		targetKey := &model.ConfigKey{
			BranchID:    config.BranchID,
			ComponentID: task.ComponentID,
			ID:          task.ConfigID,
		}

		if targetConfig, found := m.state.Get(targetKey); found {
			// Get the branch path as base for relative paths
			branchKey := model.BranchKey{ID: config.BranchID}
			if branchState, branchFound := m.state.Get(branchKey); branchFound {
				branchPath := branchState.Path()
				// Target path relative from branch directory
				if strings.HasPrefix(targetConfig.Path(), branchPath+"/") {
					taskYAML.Config = strings.TrimPrefix(targetConfig.Path(), branchPath+"/")
				} else {
					taskYAML.Config = targetConfig.Path()
				}
			}
		}
	} else if task.ConfigData != nil {
		// For inline config, serialize the config data - preserve ordering
		taskYAML.Parameters = task.ConfigData.Clone()
	}

	return taskYAML
}

// buildSchedulesYAML returns schedules for an orchestrator config.
// It first checks if schedules were pre-collected by the orchestrator mapper (in AfterRemoteOperation).
// This is necessary because the ignore mapper removes scheduler configs from state before MapBeforeLocalSave runs.
func (m *coreFilesMapper) buildSchedulesYAML(config *model.Config) []model.ScheduleYAML {
	// Use pre-collected schedules from orchestrator mapper (populated in AfterRemoteOperation)
	if config.Orchestration != nil && len(config.Orchestration.Schedules) > 0 {
		return config.Orchestration.Schedules
	}

	schedules := make([]model.ScheduleYAML, 0)

	// Fallback: Find all scheduler configs that target this orchestrator
	// This path is used when schedules weren't pre-collected (e.g., during local load)
	for _, objectState := range m.state.All() {
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		// Check if this is a scheduler config
		targetConfig := configState.LocalOrRemoteState().(*model.Config)
		component, err := m.state.Components().GetOrErr(targetConfig.ComponentID)
		if err != nil || !component.IsScheduler() {
			continue
		}

		// Check if this scheduler targets our orchestrator
		for _, rel := range targetConfig.Relations.GetByType(model.SchedulerForRelType) {
			schedulerFor := rel.(*model.SchedulerForRelation)
			if schedulerFor.ComponentID == config.ComponentID && schedulerFor.ConfigID == config.ID {
				// Convert to YAML format
				scheduleYAML := m.buildScheduleYAML(targetConfig)
				schedules = append(schedules, scheduleYAML)
				break
			}
		}
	}

	return schedules
}

// buildScheduleYAML converts a scheduler config to ScheduleYAML.
func (m *coreFilesMapper) buildScheduleYAML(config *model.Config) model.ScheduleYAML {
	schedule := model.ScheduleYAML{
		Name: config.Name,
		// Include the scheduler config ID for tracking during push operations
		Keboola: &model.ScheduleKeboolaMeta{
			ConfigID: config.ID.String(),
		},
	}

	// Get description
	if config.Description != "" {
		schedule.Description = config.Description
	}

	// Get schedule (cron) details from the schedule key
	if config.Content != nil {
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
	}

	return schedule
}

// buildConfigRowYAML constructs the ConfigYAML structure from a ConfigRow object.
func (m *coreFilesMapper) buildConfigRowYAML(configRow *model.ConfigRow) *model.ConfigYAML {
	configYAML := &model.ConfigYAML{
		Version:     2,
		Name:        configRow.Name,
		Description: strings.TrimRight(configRow.Description, " \r\n\t"),
		Disabled:    configRow.IsDisabled,
		Keboola: &model.KeboolaMetaYAML{
			ComponentID: configRow.ComponentID.String(),
			ConfigID:    configRow.ID.String(),
		},
	}

	// Extract configuration from Content
	if configRow.Content != nil {
		// Extract parameters - preserve ordering
		if params, ok := configRow.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(*orderedmap.OrderedMap); ok {
				configYAML.Parameters = paramsMap.Clone()
			} else if paramsMapAny, ok := params.(map[string]any); ok {
				// Convert regular map to orderedmap (will sort alphabetically, but at least be consistent)
				configYAML.Parameters = orderedmap.FromPairs(mapToPairs(paramsMapAny))
			}
		}
	}

	return configYAML
}

// deleteOldCoreFiles marks old core files for deletion.
func (m *coreFilesMapper) deleteOldCoreFiles(recipe *model.LocalSaveRecipe) {
	basePath := recipe.Path()

	// Delete old meta.json
	metaPath := m.state.NamingGenerator().MetaFilePath(basePath)
	if m.state.ObjectsRoot().IsFile(context.Background(), metaPath) {
		recipe.ToDelete = append(recipe.ToDelete, metaPath)
	}

	// Delete old config.json
	configPath := m.state.NamingGenerator().ConfigFilePath(basePath)
	if m.state.ObjectsRoot().IsFile(context.Background(), configPath) {
		recipe.ToDelete = append(recipe.ToDelete, configPath)
	}

	// Delete old description.md
	descPath := m.state.NamingGenerator().DescriptionFilePath(basePath)
	if m.state.ObjectsRoot().IsFile(context.Background(), descPath) {
		recipe.ToDelete = append(recipe.ToDelete, descPath)
	}
}

// orderedMapToMap converts an orderedmap to a regular map.
// Deprecated: Use orderedmap directly to preserve key ordering.
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

// mapToPairs converts a regular map to orderedmap pairs (sorted alphabetically for consistency).
func mapToPairs(m map[string]any) []orderedmap.Pair {
	if m == nil {
		return nil
	}

	pairs := make([]orderedmap.Pair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, orderedmap.Pair{Key: k, Value: v})
	}
	return pairs
}

// extractStorageInput extracts input storage mapping from storage orderedmap.
func extractStorageInput(storage *orderedmap.OrderedMap) *model.StorageInputYAML {
	inputRaw, ok := storage.Get("input")
	if !ok {
		return nil
	}
	inputMap, ok := inputRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil
	}

	input := &model.StorageInputYAML{}

	// Extract tables
	if tablesRaw, ok := inputMap.Get("tables"); ok {
		if tables, ok := tablesRaw.([]any); ok {
			for _, t := range tables {
				if tableMap, ok := t.(*orderedmap.OrderedMap); ok {
					table := model.InputTableYAML{}
					if v, ok := tableMap.Get("source"); ok {
						table.Source, _ = v.(string)
					}
					if v, ok := tableMap.Get("destination"); ok {
						table.Destination, _ = v.(string)
					}
					if v, ok := tableMap.Get("columns"); ok {
						if cols, ok := v.([]any); ok {
							for _, c := range cols {
								if s, ok := c.(string); ok {
									table.Columns = append(table.Columns, s)
								}
							}
						}
					}
					if v, ok := tableMap.Get("where_column"); ok {
						table.WhereColumn, _ = v.(string)
					}
					if v, ok := tableMap.Get("where_operator"); ok {
						table.WhereOperator, _ = v.(string)
					}
					if v, ok := tableMap.Get("limit"); ok {
						if l, ok := v.(int); ok {
							table.Limit = l
						} else if l, ok := v.(float64); ok {
							table.Limit = int(l)
						}
					}
					input.Tables = append(input.Tables, table)
				}
			}
		}
	}

	if len(input.Tables) == 0 && len(input.Files) == 0 {
		return nil
	}
	return input
}

// extractStorageOutput extracts output storage mapping from storage orderedmap.
func extractStorageOutput(storage *orderedmap.OrderedMap) *model.StorageOutputYAML {
	outputRaw, ok := storage.Get("output")
	if !ok {
		return nil
	}
	outputMap, ok := outputRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil
	}

	output := &model.StorageOutputYAML{}

	// Extract tables
	if tablesRaw, ok := outputMap.Get("tables"); ok {
		if tables, ok := tablesRaw.([]any); ok {
			for _, t := range tables {
				if tableMap, ok := t.(*orderedmap.OrderedMap); ok {
					table := model.OutputTableYAML{}
					if v, ok := tableMap.Get("source"); ok {
						table.Source, _ = v.(string)
					}
					if v, ok := tableMap.Get("destination"); ok {
						table.Destination, _ = v.(string)
					}
					if v, ok := tableMap.Get("primary_key"); ok {
						if pks, ok := v.([]any); ok {
							for _, pk := range pks {
								if s, ok := pk.(string); ok {
									table.PrimaryKey = append(table.PrimaryKey, s)
								}
							}
						}
					}
					if v, ok := tableMap.Get("incremental"); ok {
						table.Incremental, _ = v.(bool)
					}
					output.Tables = append(output.Tables, table)
				}
			}
		}
	}

	if len(output.Tables) == 0 && len(output.Files) == 0 {
		return nil
	}
	return output
}

// isSchedulerForOrchestrator checks if the config is a scheduler targeting an orchestrator.
func (m *coreFilesMapper) isSchedulerForOrchestrator(config *model.Config) bool {
	// Check if this is a scheduler component
	component, err := m.state.Components().GetOrErr(config.ComponentID)
	if err != nil || !component.IsScheduler() {
		return false
	}

	// Check if it has a SchedulerForRelation targeting an orchestrator
	for _, rel := range config.Relations.GetByType(model.SchedulerForRelType) {
		schedulerFor := rel.(*model.SchedulerForRelation)
		targetComponent, err := m.state.Components().GetOrErr(schedulerFor.ComponentID)
		if err == nil && orchestrator.IsOrchestratorOrFlow(targetComponent) {
			return true
		}
	}

	return false
}
