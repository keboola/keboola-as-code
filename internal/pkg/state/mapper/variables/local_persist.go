package variables

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m *variablesLocalMapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	// Variables are represented by config
	variablesKey, ok := recipe.Key.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Parent of the variables must be config that using variables
	configKey, ok := recipe.ParentKey.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Get components
	components, err := m.Components()
	if err != nil {
		return err
	}

	// Get component
	component, err := components.Get(variablesKey.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "variables"
	if !component.IsVariables() {
		return nil
	}

	// Branch must be same
	if variablesKey.BranchKey() != configKey.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, variablesKey.String(), configKey.String()))
	}

	// Add relation
	recipe.Relations.Add(&model.VariablesForRelation{
		ComponentId: configKey.ComponentId,
		ConfigId:    configKey.Id,
	})

	return nil
}

// AfterLocalPersist ensures there is one config row with default variables values after persist.
func (m *variablesLocalMapper) AfterLocalPersist(persisted []model.Object) error {
	// Find persisted configs
	errors := utils.NewMultiError()
	configs, err := m.findPersistedConfigs(persisted)
	if err != nil {
		errors.Append(err)
	}

	// Ensure that each "variables" config has one row with default values
	for _, config := range configs {
		if err := m.ensureOneRowHasRelation(config); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

// ensureOneRowHasRelation VariablesValuesForRelation, it marks variables default values.
func (m *variablesLocalMapper) ensureOneRowHasRelation(config *model.Config) error {
	configRelation, err := config.GetRelations().GetOneByType(model.VariablesForRelType)
	if err != nil || configRelation == nil {
		return err
	}

	// Process rows
	rows := m.state.ConfigRowsFrom(config.ConfigKey)
	var rowsWithRelation []*model.ConfigRow
	var rowsWithDefaultInName []*model.ConfigRow
	for _, row := range rows {
		// Has row relation?
		rowRelation, err := row.GetRelations().GetOneByType(model.VariablesValuesForRelType)
		if err != nil {
			return err
		}
		if rowRelation != nil {
			rowsWithRelation = append(rowsWithRelation, row)
		}

		// Has row "default" in the name or path?
		if strings.Contains(strhelper.NormalizeName(row.Name), `default`) {
			rowsWithDefaultInName = append(rowsWithDefaultInName, row)
		}
	}

	// Row with relation already exists -> end
	if len(rowsWithRelation) > 0 {
		return nil
	}

	// Determine row with default values
	var row *model.ConfigRow
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
	row.AddRelation(&model.VariablesValuesForRelation{})
	return nil
}

// findPersistedConfigs returns all persisted configs + configs with a persisted row.
func (m *variablesLocalMapper) findPersistedConfigs(persisted []model.Object) ([]*model.Config, error) {
	components, err := m.Components()
	if err != nil {
		return nil, err
	}

	// Find new persisted variables configs + include those that have a new persisted row
	configsMap := make(map[model.ConfigKey]bool)
	errors := utils.NewMultiError()
	for _, object := range persisted {
		if config, ok := object.(*model.Config); ok {
			// Variables config?
			component, err := components.Get(config.ComponentKey())
			if err != nil {
				errors.Append(err)
				continue
			}
			if component.IsVariables() {
				configsMap[config.ConfigKey] = true
			}
		} else if row, ok := object.(*model.ConfigRow); ok {
			// Variables values row?
			component, err := components.Get(row.ComponentKey())
			if err != nil {
				errors.Append(err)
				continue
			}
			if component.IsVariables() {
				configsMap[row.ConfigKey()] = true
			}
		}
	}

	// Convert map to slice
	var out []*model.Config
	for configKey := range configsMap {
		out = append(out, m.state.MustGet(configKey).(*model.Config))
	}
	return out, errors.ErrorOrNil()
}
