package variables

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m *variablesMapper) MapBeforePersist(ctx context.Context, recipe *model.PersistRecipe) error {
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
	component, err := m.state.Components().GetOrErr(configManifest.ComponentID)
	if err != nil {
		return err
	}

	// Component must be "variables"
	if !component.IsVariables() {
		return nil
	}

	// Branch must be same
	if configKey.BranchKey() != configManifest.BranchKey() {
		panic(errors.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), configKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.VariablesForRelation{
		ComponentID: configKey.ComponentID,
		ConfigID:    configKey.ID,
	})

	return nil
}

// AfterLocalOperation ensures there is one config row with default variables values after persist.
func (m *variablesMapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
	// Find new persisted variables configs + include those that have a new persisted row
	configs := make(map[model.ConfigKey]bool)
	errs := errors.NewMultiError()
	for _, objectState := range changes.Persisted() {
		object := objectState.LocalState()
		if config, ok := object.(*model.Config); ok {
			// Variables config?
			component, err := m.state.Components().GetOrErr(config.ComponentID)
			if err != nil {
				errs.Append(err)
				continue
			}
			if component.IsVariables() {
				configs[config.ConfigKey] = true
			}
		} else if row, ok := object.(*model.ConfigRow); ok {
			// Variables values row?
			component, err := m.state.Components().GetOrErr(row.ComponentID)
			if err != nil {
				errs.Append(err)
				continue
			}
			if component.IsVariables() {
				configs[row.ConfigKey()] = true
			}
		}
	}

	// Ensure that each variables config has one row with default values
	for configKey := range configs {
		config := m.state.MustGet(configKey).LocalState().(*model.Config)
		if err := m.ensureOneRowHasRelation(config); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

// ensureOneRowHasRelation VariablesValuesForRelation, it marks variables default values.
func (m *variablesMapper) ensureOneRowHasRelation(config *model.Config) error {
	configRelation, err := config.GetRelations().GetOneByType(model.VariablesForRelType)
	if err != nil || configRelation == nil {
		return err
	}

	// Process rows
	rows := m.state.ConfigRowsFrom(config.ConfigKey)
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
		if strings.Contains(strhelper.NormalizeName(row.Local.Name), `default`) ||
			strings.Contains(row.GetRelativePath(), `default`) {
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
	row.Local.AddRelation(&model.VariablesValuesForRelation{})
	row.AddRelation(&model.VariablesValuesForRelation{})
	return nil
}
