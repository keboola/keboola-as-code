package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapBeforeLocalSave - replace shared codes IDs by paths on local save.
func (m *mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Shared code can be used only by transformation - transformation struct must be set
	transformation, ok := recipe.Object.(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

	// Link to shared code must be set
	if transformation.Transformation.LinkToSharedCode == nil {
		return nil
	}

	if err := m.replaceSharedCodeIdByPath(transformation, recipe); err != nil {
		// Log errors as warning
		m.Logger.Warn(utils.PrefixError(`Warning`, err))
	}

	return nil
}

func (m *mapper) replaceSharedCodeIdByPath(transformation *model.Config, recipe *model.LocalSaveRecipe) error {
	// Get config file
	configFile, err := recipe.Files.ObjectConfigFile()
	if err != nil {
		// nolint: nilerr
		return nil
	}

	// Get shared code
	sharedCodeKey := transformation.Transformation.LinkToSharedCode.Config
	sharedCodeState, found := m.State.GetOrNil(sharedCodeKey).(*model.ConfigState)
	if !found {
		return utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, sharedCodeKey.Desc()),
			fmt.Errorf(`referenced from %s`, transformation.Desc()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.CheckTargetComponent(sharedCodeState.LocalOrRemoteState().(*model.Config), transformation.ConfigKey); err != nil {
		return err
	}

	// Replace Shared Code ID -> Shared Code Path
	configFile.Content.Set(model.SharedCodePathContentKey, sharedCodeState.GetObjectPath())

	// Replace IDs -> paths in scripts
	errors := utils.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		if v, err := m.replaceIdByPathInScript(code, script, sharedCodeState); err != nil {
			errors.Append(err)
		} else if v != nil {
			return v
		}
		return script
	})
	return errors.ErrorOrNil()
}
