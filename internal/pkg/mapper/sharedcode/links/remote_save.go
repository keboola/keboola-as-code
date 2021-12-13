package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave move shared code from Transformation struct to Content.
func (m *mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Shared code can be used only by transformation - struct must be set
	transformation, ok := recipe.ApiObject.(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

	// Link to shared code must be set
	if transformation.Transformation.LinkToSharedCode == nil {
		return nil
	}
	sharedCodeLink := transformation.Transformation.LinkToSharedCode

	// Clear link to shared code
	defer func() {
		transformation.Transformation.LinkToSharedCode = nil
	}()

	// Set shared code config ID and rows IDs
	// Note: IDs are already validated on remote/local load
	transformation.Content.Set(model.SharedCodeIdContentKey, sharedCodeLink.Config.Id.String())
	transformation.Content.Set(model.SharedCodeRowsIdContentKey, sharedCodeLink.Rows.IdsSlice())

	return nil
}
