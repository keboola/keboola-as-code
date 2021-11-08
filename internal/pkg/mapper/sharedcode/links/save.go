package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	if err := m.replaceSharedCodeIdByPath(recipe); err != nil {
		// Log errors as warning
		m.Logger.Warn(utils.PrefixError(`Warning`, err))
	}

	return nil
}

func (m *mapper) replaceSharedCodeIdByPath(recipe *model.LocalSaveRecipe) error {
	// Shared code is used by config
	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Config have to be from transformation component
	component, err := m.State.Components().Get(config.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsTransformation() {
		return nil
	}

	// ID is stored in configuration
	sharedCodeIdRaw, found := recipe.Configuration.Content.Get(model.SharedCodeIdContentKey)
	if !found {
		return nil
	}

	// ID must be string
	sharedCodeConfigId, ok := sharedCodeIdRaw.(string)
	if !ok {
		return nil
	}

	// Remove shared code id
	defer func() {
		recipe.Configuration.Content.Delete(model.SharedCodeIdContentKey)
	}()

	// Load shared code config
	sharedCodeKey := model.ConfigKey{
		BranchId:    config.BranchId, // same branch
		ComponentId: model.SharedCodeComponentId,
		Id:          sharedCodeConfigId,
	}
	sharedCodeRaw, found := m.State.Get(sharedCodeKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`missing shared code %s`, sharedCodeKey.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, config.Desc()))
		return errors
	}
	sharedCodeState := sharedCodeRaw.(*model.ConfigState)
	sharedCode := sharedCodeState.LocalOrRemoteState().(*model.Config)
	targetComponentId, err := m.getSharedCodeTargetComponentId(sharedCode)
	if err != nil {
		return err
	}

	// Check componentId
	if targetComponentId != config.ComponentId {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.SharedCodeComponentIdContentKey, sharedCodeState.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - expected "%s"`, config.ComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - found "%s"`, targetComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, config.Desc()))
		return errors
	}

	// Replace Shared Code ID -> Shared Code Path
	recipe.Configuration.Content.Set(model.SharedCodePathContentKey, sharedCodeState.GetObjectPath())
	return nil
}
