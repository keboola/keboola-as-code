package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeRemoteSave move shared code from Transformation struct to Content.
func (m *mapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
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
	errs := errors.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		v, err := m.linkToIDPlaceholder(code, script)
		if err != nil {
			errs.Append(err)
		}
		if v != nil {
			return v
		}
		return script
	})

	// Set shared code config ID and rows IDs
	// Note: IDs are already validated on remote/local load
	transformation.Content.Set(model.SharedCodeIDContentKey, sharedCodeLink.Config.ID.String())
	transformation.Content.Set(model.SharedCodeRowsIDContentKey, sharedCodeLink.Rows.IdsSlice())

	return errs.ErrorOrNil()
}
