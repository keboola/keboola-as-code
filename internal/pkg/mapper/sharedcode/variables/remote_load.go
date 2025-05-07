package variables

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterRemoteLoad - extract shared code "variables_id".
func (m *mapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}
	object := recipe.Object.(*model.ConfigRow)

	// Variables ID must be string
	variablesID, ok := m.GetSharedCodeVariablesID(object)
	if !ok {
		return nil
	}

	// Create relation
	object.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesID: keboola.ConfigID(variablesID),
	})

	// Remove variables ID from configuration content
	object.Content.Delete(model.SharedCodeVariablesIDContentKey)
	return nil
}
