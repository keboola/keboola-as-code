package variables

import "github.com/keboola/keboola-as-code/internal/pkg/model"

func (m *variablesMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Variables are used by config
	apiObject, ok := recipe.ApiObject.(*model.Config)
	if !ok {
		return nil
	}

	// Variables ID is stored in configuration
	variablesIdRaw, found := apiObject.Content.Get(model.VariablesIdContentKey)
	if !found {
		return nil
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return nil
	}

	// Create relation
	internalObject := recipe.InternalObject.(*model.Config)
	internalObject.AddRelation(&model.VariablesFromRelation{
		Source: model.ConfigKeySameBranch{
			ComponentId: model.VariablesComponentId,
			Id:          variablesId,
		},
	})

	// Remove variables ID from configuration content
	internalObject.Content.Delete(model.VariablesIdContentKey)
	return nil
}
