package variables

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *variablesMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
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

func (m *variablesMapper) saveVariables(config *model.Config, recipe *model.RemoteSaveRecipe) (*model.VariablesFromRelation, error) {
	// Get relation
	relType := model.VariablesFromRelType
	relationRaw, err := config.Relations.GetOneByType(relType)
	if err != nil {
		return nil, errors.Errorf(`unexpected state of %s: %w`, recipe.Desc(), err)
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

func (m *variablesMapper) saveVariablesValues(config *model.Config, recipe *model.RemoteSaveRecipe) error {
	// Get relation
	relType := model.VariablesValuesFromRelType
	relationRaw, err := config.Relations.GetOneByType(relType)
	if err != nil {
		return errors.Errorf(`unexpected state of %s: %w`, recipe.Desc(), err)
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
