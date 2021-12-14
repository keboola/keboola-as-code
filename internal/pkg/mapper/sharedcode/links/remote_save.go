package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapBeforeRemoteSave move shared code from Transformation struct to Content.
func (m *mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Shared code can be used only by transformation - struct must be set
	transformation, ok := recipe.Object.(*model.Config)
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

	// Convert LinkScript to ID placeholder
	errors := utils.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		v, err := m.linkToIdPlaceholder(code, script)
		if err != nil {
			errors.Append(err)
		}
		if v != nil {
			return v
		}
		return script
	})

	// Set shared code config ID and rows IDs
	// Note: IDs are already validated on remote/local load
	transformation.Content.Set(model.SharedCodeIdContentKey, sharedCodeLink.Config.Id.String())
	transformation.Content.Set(model.SharedCodeRowsIdContentKey, sharedCodeLink.Rows.IdsSlice())

	return errors.ErrorOrNil()
}
