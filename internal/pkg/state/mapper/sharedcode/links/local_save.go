package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave - replace shared codes IDs by paths on local save.
func (m *localMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Shared code can be used only by transformation - transformation struct must be set
	transformation, ok := recipe.Object.(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

	// Link to shared code must be set
	if transformation.Transformation.LinkToSharedCode == nil {
		return nil
	}

	if err := m.replaceSharedCodeIdByPath(transformation); err != nil {
		// Log errors as warning
		m.logger.Warn(errors.PrefixError(`Warning`, err))
	}

	return nil
}

func (m *localMapper) replaceSharedCodeIdByPath(transformation *model.Config) error {
	// Get shared code
	sharedCodeKey := transformation.Transformation.LinkToSharedCode.Config
	sharedCode, found := m.state.GetOrNil(sharedCodeKey).(*model.Config)

	// Convert LinkScript to path placeholder
	errs := errors.NewMultiError()
	transformation.Transformation.MapScripts(func(_ *model.Block, code *model.Code, script model.Script) model.Script {
		if v, err := m.linkToPathPlaceholder(code, script, sharedCode); err != nil {
			errs.Append(err)
		} else if v != nil {
			return v
		}
		return script
	})

	// Check if shared code is found
	if !found {
		return errors.PrefixError(
			fmt.Sprintf(`missing shared code %s`, sharedCodeKey.String()),
			fmt.Errorf(`referenced from %s`, transformation.String()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCode, transformation.ConfigKey); err != nil {
		return err
	}

	// Shared code path
	sharedCodePath, err := m.state.GetPath(sharedCode)
	if err != nil {
		return err
	}

	// Replace Shared Code ID -> Shared Code path
	transformation.Content.Set(model.SharedCodePathContentKey, sharedCodePath.RelativePath())

	return errs.ErrorOrNil()
}
