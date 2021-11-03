package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *variablesMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Variables are used by config
	apiObject, ok := recipe.ApiObject.(*model.Config)
	if !ok {
		return nil
	}
	internalObject := recipe.InternalObject.(*model.Config)

	m.loadVariables(apiObject, internalObject)
	m.loadVariablesValues(apiObject, internalObject)
	return nil
}

func (m *variablesMapper) loadVariables(apiObject, internalObject *model.Config) {
	// Variables ID is stored in configuration
	variablesIdRaw, found := apiObject.Content.Get(model.VariablesIdContentKey)
	if !found {
		return
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return
	}

	// Create relation
	internalObject.AddRelation(&model.VariablesFromRelation{
		Source: model.ConfigKeySameBranch{
			ComponentId: model.VariablesComponentId,
			Id:          variablesId,
		},
	})

	// Remove variables ID from configuration content
	internalObject.Content.Delete(model.VariablesIdContentKey)
}

func (m *variablesMapper) loadVariablesValues(apiObject, internalObject *model.Config) {
	// Values ID is stored in configuration
	valuesIdRaw, found := apiObject.Content.Get(model.VariablesValuesIdContentKey)
	if !found {
		return
	}

	// Values ID must be string
	valuesId, ok := valuesIdRaw.(string)
	if !ok {
		return
	}

	// Config must have define variables config
	variablesRelations := internalObject.Relations.GetByType(model.VariablesFromRelType)
	if len(variablesRelations) != 1 {
		return
	}
	variablesRelation := variablesRelations[0].(*model.VariablesFromRelation)

	// Create relation
	internalObject.AddRelation(&model.VariablesValuesFromRelation{
		Source: model.ConfigRowKeySameBranch{
			ComponentId: variablesRelation.Source.ComponentId,
			ConfigId:    variablesRelation.Source.Id,
			Id:          valuesId,
		},
	})

	// Remove variables ID from configuration content
	internalObject.Content.Delete(model.VariablesValuesIdContentKey)
}
