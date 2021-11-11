package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
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
	transformation, sharedCodeKey, err := m.GetSharedCodeKey(recipe.Object)
	if err != nil || transformation == nil {
		return err
	}

	// Remove shared code id
	defer func() {
		recipe.Configuration.Content.Delete(model.SharedCodeIdContentKey)
		recipe.Configuration.Content.Delete(model.SharedCodeRowsIdContentKey)
	}()

	// Load shared code config
	sharedCodeRaw, found := m.State.Get(sharedCodeKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`missing shared code %s`, sharedCodeKey.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, transformation.Desc()))
		return errors
	}
	sharedCodeState := sharedCodeRaw.(*model.ConfigState)
	sharedCode := sharedCodeState.LocalOrRemoteState().(*model.Config)
	targetComponentId, err := m.GetTargetComponentId(sharedCode)
	if err != nil {
		return err
	}

	// Check componentId
	if targetComponentId != transformation.ComponentId {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.SharedCodeComponentIdContentKey, sharedCodeState.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - expected "%s"`, transformation.ComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - found "%s"`, targetComponentId))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, transformation.Desc()))
		return errors
	}

	// Replace Shared Code ID -> Shared Code Path
	recipe.Configuration.Content.Set(model.SharedCodePathContentKey, sharedCodeState.GetObjectPath())

	// Replace IDs -> paths in scripts
	errors := utils.NewMultiError()
	for _, block := range transformation.Blocks {
		for _, code := range block.Codes {
			for index, script := range code.Scripts {
				if v, err := m.replaceIdByPathInScript(script, code, sharedCodeState); err != nil {
					errors.Append(err)
					continue
				} else if v != "" {
					code.Scripts[index] = v
				}
			}
		}
	}
	return errors.ErrorOrNil()
}

func (m *mapper) replaceIdByPathInScript(script string, code *model.Code, sharedCode *model.ConfigState) (string, error) {
	id := m.matchId(script)
	if id == "" {
		// Not found
		return "", nil
	}

	// Get shared code config row
	rowKey := model.ConfigRowKey{
		BranchId:    sharedCode.BranchId,
		ComponentId: sharedCode.ComponentId,
		ConfigId:    sharedCode.Id,
		Id:          id,
	}
	row, found := m.State.Get(rowKey)
	if !found {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`shared code %s not found`, rowKey.Desc()))
		errors.AppendRaw(fmt.Sprintf(`  - referenced from %s`, code.Desc()))
		return "", errors
	}

	// Return path instead of ID
	path, err := filesystem.Rel(sharedCode.Path(), row.Path())
	if err != nil {
		return "", err
	}
	return m.formatPath(path, code.ComponentId), nil
}
