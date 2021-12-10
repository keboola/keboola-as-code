package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *variablesMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Variables are used by config
	internalObject, ok := recipe.InternalObject.(*model.Config)
	if !ok {
		return nil
	}
	apiObject := recipe.ApiObject.(*model.Config)

	// Save variables_id
	errors := utils.NewMultiError()
	variablesRelation, err := m.saveVariables(apiObject, internalObject, recipe)
	if err != nil {
		errors.Append(err)
	}

	// Save variables_values_id if variables are present
	if variablesRelation != nil {
		if err := m.saveVariablesValues(apiObject, internalObject, recipe); err != nil {
			errors.Append(err)
		}
	}

	return nil
}

func (m *variablesMapper) saveVariables(apiObject, internalObject *model.Config, recipe *model.RemoteSaveRecipe) (*model.VariablesFromRelation, error) {
	// Get relation
	relType := model.VariablesFromRelType
	relationRaw, err := internalObject.Relations.GetOneByType(relType)
	if err != nil {
		return nil, fmt.Errorf(`unexpected state of %s: %w`, recipe.Manifest.Desc(), err)
	} else if relationRaw == nil {
		return nil, nil
	}
	relation := relationRaw.(*model.VariablesFromRelation)

	// Set variables ID
	apiObject.Content.Set(model.VariablesIdContentKey, relation.VariablesId.String())

	// Delete relation
	apiObject.Relations.RemoveByType(relType)
	return relation, nil
}

func (m *variablesMapper) saveVariablesValues(apiObject, internalObject *model.Config, recipe *model.RemoteSaveRecipe) error {
	// Get relation
	relType := model.VariablesValuesFromRelType
	relationRaw, err := internalObject.Relations.GetOneByType(relType)
	if err != nil {
		return fmt.Errorf(`unexpected state of %s: %w`, recipe.Manifest.Desc(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.VariablesValuesFromRelation)

	// Set values ID
	apiObject.Content.Set(model.VariablesValuesIdContentKey, relation.VariablesValuesId.String())

	// Delete relation
	apiObject.Relations.RemoveByType(relType)
	return nil
}
