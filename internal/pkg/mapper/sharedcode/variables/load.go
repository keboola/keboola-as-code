package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterRemoteLoad - extract shared code "variables_id".
func (m *mapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	if ok, err := m.IsSharedCodeRowKey(recipe.InternalObject.Key()); err != nil || !ok {
		return err
	}
	internalObject := recipe.InternalObject.(*model.ConfigRow)

	// Variables ID must be string
	variablesId, ok := m.GetSharedCodeVariablesId(internalObject)
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
