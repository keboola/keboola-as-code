package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapBeforeRemoteSave move shared code from Transformation struct to Content.
func (m *remoteMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
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
	transformation.Transformation.MapScripts(func(_ *model.Block, code *model.Code, script model.Script) model.Script {
		v, err := m.linkToIdPlaceholder(code, script)
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
	transformation.Content.Set(model.SharedCodeIdContentKey, sharedCodeLink.Config.Id.String())
	transformation.Content.Set(model.SharedCodeRowsIdContentKey, sharedCodeLink.Rows.IdsSlice())

	return errs.ErrorOrNil()
}

func (m *remoteMapper) linkToIdPlaceholder(code *model.Code, script model.Script) (model.Script, error) {
	if link, ok := script.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.ConfigRow)
		script := model.StaticScript{Value: m.id.format(row.ConfigRowId)}
		if !ok {
			return script, errors.PrefixError(
				fmt.Sprintf(`missing shared code "%s"`, link.Target.String()),
				fmt.Errorf(`referenced from %s`, code.String()),
			)
		}
		return script, nil
	}
	return nil, nil
}
