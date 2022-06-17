package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterRemoteLoad - extract shared code "variables_id".
func (m *mapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}
	object := recipe.Object.(*model.ConfigRow)

	// Variables ID must be string
	variablesId, ok := m.GetSharedCodeVariablesId(object)
	if !ok {
		return nil
	}

	// Create relation
	object.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: storageapi.ConfigID(variablesId),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.SharedCodeVariablesIdContentKey)
	return nil
}
