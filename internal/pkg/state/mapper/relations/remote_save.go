package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave - modify changed fields.
func (m *relationsMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	if recipe.ChangedFields.Has(`relations`) {
		// Relations are stored on the API side in config/row configuration
		recipe.ChangedFields.Add(`configuration`)
		recipe.ChangedFields.Remove(`relations`)
	}
	return nil
}
