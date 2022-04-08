package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *variablesRemoteMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Variables are used by config
	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Save variables_id
	errs := errors.NewMultiError()
	variablesRelation, err := m.saveVariables(config, recipe)
	if err != nil {
		errs.Append(err)
	}

	// Save variables_values_id if variables are present
	if variablesRelation != nil {
		if err := m.saveVariablesValues(config, recipe); err != nil {
			errs.Append(err)
		}
	}

	return nil
}

func (m *variablesRemoteMapper) saveVariables(config *model.Config, recipe *model.RemoteSaveRecipe) (*model.VariablesFromRelation, error) {
	// Get relation
	relType := model.VariablesFromRelType
	relationRaw, err := config.Relations.GetOneByType(relType)
	if err != nil {
		return nil, fmt.Errorf(`unexpected state of %s: %w`, recipe.Object.String(), err)
	} else if relationRaw == nil {
		return nil, nil
	}
	relation := relationRaw.(*model.VariablesFromRelation)

	// Set variables ID
	config.Content.Set(model.VariablesIdContentKey, relation.VariablesId.String())

	// Delete relation
	config.Relations.RemoveByType(relType)
	return relation, nil
}

func (m *variablesRemoteMapper) saveVariablesValues(config *model.Config, recipe *model.RemoteSaveRecipe) error {
	// Get relation
	relType := model.VariablesValuesFromRelType
	relationRaw, err := config.Relations.GetOneByType(relType)
	if err != nil {
		return fmt.Errorf(`unexpected state of %s: %w`, recipe.Object.String(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.VariablesValuesFromRelation)

	// Set values ID
	config.Content.Set(model.VariablesValuesIdContentKey, relation.VariablesValuesId.String())

	// Delete relation
	config.Relations.RemoveByType(relType)
	return nil
}
