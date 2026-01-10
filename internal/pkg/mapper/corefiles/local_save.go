package corefiles

import (
	"context"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
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

	// Extract configuration from Content (orderedmap)
	if config.Content != nil {
		// Extract storage input/output
		if storage, ok := config.Content.Get("storage"); ok {
			if storageMap, ok := storage.(*orderedmap.OrderedMap); ok {
				configYAML.Input = extractStorageInput(storageMap)
				configYAML.Output = extractStorageOutput(storageMap)
			}
		}

		// Extract parameters
		if params, ok := config.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(*orderedmap.OrderedMap); ok {
				configYAML.Parameters = orderedMapToMap(paramsMap)
			} else if paramsMapAny, ok := params.(map[string]any); ok {
				configYAML.Parameters = paramsMapAny
			}
		}

		// Extract runtime/backend
		if runtime, ok := config.Content.Get("runtime"); ok {
			if runtimeMap, ok := runtime.(*orderedmap.OrderedMap); ok {
				backend := &model.BackendYAML{}
				if backendType, ok := runtimeMap.Get("backend"); ok {
					if backendMap, ok := backendType.(*orderedmap.OrderedMap); ok {
						if t, ok := backendMap.Get("type"); ok {
							backend.Type, _ = t.(string)
						}
						if c, ok := backendMap.Get("context"); ok {
							backend.Context, _ = c.(string)
						}
					}
				}
				if backend.Type != "" || backend.Context != "" {
					configYAML.Backend = backend
				}
			}
		}
	}

	return configYAML
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
		// Extract parameters
		if params, ok := configRow.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(*orderedmap.OrderedMap); ok {
				configYAML.Parameters = orderedMapToMap(paramsMap)
			} else if paramsMapAny, ok := params.(map[string]any); ok {
				configYAML.Parameters = paramsMapAny
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
