package variables

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *variablesMapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	// Variables are represented by config
	configManifest, ok := recipe.Manifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	// Parent of the variables must be config that using variables
	configKey, ok := recipe.ParentKey.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Get component
	component, err := m.State.Components().Get(configManifest.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "variables"
	if !component.IsVariables() {
		return nil
	}

	// Branch must be same
	if configKey.BranchKey() != configManifest.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), configKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.VariablesForRelation{
		ComponentId: configKey.ComponentId,
		ConfigId:    configKey.Id,
	})

	return nil
}

// OnObjectsPersist ensures there is one config row with default variables values.
func (m *variablesMapper) OnObjectsPersist(event model.OnObjectsPersistEvent) error {
	// Find new persisted variables configs + include those that have a new persisted row
	configs := make(map[model.ConfigKey]bool)
	errors := utils.NewMultiError()
	for _, object := range event.PersistedObjects {
		if config, ok := object.(*model.Config); ok {
			// Variables config?
			component, err := m.State.Components().Get(config.ComponentKey())
			if err != nil {
				errors.Append(err)
				continue
			}
			if component.IsVariables() {
				configs[config.ConfigKey] = true
			}
		} else if row, ok := object.(*model.ConfigRow); ok {
			// Variables values row?
			component, err := m.State.Components().Get(row.ComponentKey())
			if err != nil {
				errors.Append(err)
				continue
			}
			if component.IsVariables() {
				configs[row.ConfigKey()] = true
			}
		}
	}

	// Ensure that each variables config has one row with default values
	for configKey := range configs {
		config := m.State.MustGet(configKey).LocalState().(*model.Config)
		if err := m.ensureOneRowHasRelation(config); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

// ensureOneRowHasRelation VariablesValuesForRelation, it marks variables default values.
func (m *variablesMapper) ensureOneRowHasRelation(config *model.Config) error {
	configRelation, err := config.GetRelations().GetOneByType(model.VariablesForRelType)
	if err != nil || configRelation == nil {
		return err
	}

	// Process rows
	rows := m.State.ConfigRowsFrom(config.ConfigKey)
	var rowsWithRelation []*model.ConfigRowState
	var rowsWithDefaultInName []*model.ConfigRowState
	for _, row := range rows {
		if !row.HasLocalState() {
			continue
		}

		// Has row relation?
		rowRelation, err := row.GetRelations().GetOneByType(model.VariablesValuesForRelType)
		if err != nil {
			return err
		}
		if rowRelation != nil {
			rowsWithRelation = append(rowsWithRelation, row)
		}

		// Has row "default" in the name or path?
		if strings.Contains(utils.NormalizeName(row.Local.Name), `default`) ||
			strings.Contains(row.GetObjectPath(), `default`) {
			rowsWithDefaultInName = append(rowsWithDefaultInName, row)
		}
	}

	// Row with relation already exists -> end
	if len(rowsWithRelation) > 0 {
		return nil
	}

	// Determine row with default values
	var row *model.ConfigRowState
	switch {
	case len(rowsWithDefaultInName) > 0:
		// Add relation to row with "default" in the name
		row = rowsWithDefaultInName[0]
	case len(rows) > 0:
		// Add relation to the first row
		row = rows[0]
	default:
		// No rows -> end
		return nil
	}

	// Add relation to row local object and manifest
	relation := &model.VariablesValuesForRelation{}
	row.Local.AddRelation(relation)
	row.ConfigRowManifest.AddRelation(relation)
	return nil
}
