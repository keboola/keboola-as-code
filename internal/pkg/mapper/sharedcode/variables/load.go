package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterRemoteLoad - extract shared code "variables_id".
func (m *mapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Variables are used by shared code - config row.
	apiObject, ok := recipe.ApiObject.(*model.ConfigRow)
	if !ok {
		return nil
	}
	internalObject := recipe.InternalObject.(*model.ConfigRow)

	// Check component type
	component, err := m.State.Components().Get(internalObject.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsSharedCode() {
		return nil
	}

	// Variables ID is stored in configuration
	variablesIdRaw, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	if !found {
		return nil
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return nil
	}

	// Create relation
	internalObject.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: variablesId,
	})

	// Remove variables ID from configuration content
	internalObject.Content.Delete(model.SharedCodeVariablesIdContentKey)
	return nil
}
