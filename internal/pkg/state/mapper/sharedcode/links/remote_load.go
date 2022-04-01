package links

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *remoteMapper) AfterRemoteOperation(changes *model.Changes) error {
	errors := utils.NewMultiError()

	// Find loaded transformations
	for _, object := range changes.Loaded() {
		// Shared code can be used only by transformation - struct must be set
		if transformation, ok := object.(*model.Config); !ok || transformation.Transformation == nil {
			return nil
		} else {
			if err := m.onRemoteLoad(transformation); err != nil {
				errors.Append(err)
			}
		}
	}

	if errors.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(utils.PrefixError(`Warning`, errors))
	}
	return nil
}

// onRemoteLoad move shared code config/rows IDs from Content to Transformation struct.
func (m *remoteMapper) onRemoteLoad(transformation *model.Config) error {
	// Always remove shared code config/rows IDs from Content
	defer func() {
		transformation.Content.Delete(model.SharedCodeIdContentKey)
		transformation.Content.Delete(model.SharedCodeRowsIdContentKey)
	}()

	// Get shared code ID
	sharedCodeIdRaw, found := transformation.Content.Get(model.SharedCodeIdContentKey)
	sharedCodeId, ok := sharedCodeIdRaw.(string)
	if !found {
		return nil
	} else if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`invalid transformation %s`, transformation.String()),
			fmt.Errorf(`key "%s" should be string, found %T`, model.SharedCodeIdContentKey, sharedCodeIdRaw),
		)
	}

	// Get shared code
	linkToSharedCode := &model.LinkToSharedCode{
		Config: model.ConfigKey{
			BranchId:    transformation.BranchId,
			ComponentId: model.SharedCodeComponentId,
			Id:          model.ConfigId(sharedCodeId),
		},
	}
	sharedCode, found := m.state.GetOrNil(linkToSharedCode.Config).(*model.Config)
	if !found {
		return utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, linkToSharedCode.Config.String()),
			fmt.Errorf(`referenced from %s`, transformation.String()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCode, transformation.ConfigKey); err != nil {
		return err
	}

	// Store shared code config key in Transformation structure
	transformation.Transformation.LinkToSharedCode = linkToSharedCode

	// Get shared code config rows IDs
	sharedCodeRowsIdsRaw, found := transformation.Content.Get(model.SharedCodeRowsIdContentKey)
	v, ok := sharedCodeRowsIdsRaw.([]interface{})
	if !found {
		return nil
	} else if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`invalid transformation %s`, transformation.String()),
			fmt.Errorf(`key "%s" should be array, found %T`, model.SharedCodeRowsIdContentKey, sharedCodeRowsIdsRaw),
		)
	}

	// Replace ID placeholder with LinkScript struct
	errors := utils.NewMultiError()
	transformation.Transformation.MapScripts(func(_ *model.Block, code *model.Code, script model.Script) model.Script {
		if _, v, err := m.parseIdPlaceholder(code, script, sharedCode); err != nil {
			errors.Append(err)
		} else if v != nil {
			return v
		}
		return script
	})

	// Check rows IDs
	for _, rowId := range v {
		rowKey := model.ConfigRowKey{
			BranchId:    linkToSharedCode.Config.BranchId,
			ComponentId: linkToSharedCode.Config.ComponentId,
			ConfigId:    linkToSharedCode.Config.Id,
			Id:          model.RowId(cast.ToString(rowId)),
		}
		if _, found := m.state.Get(rowKey); found {
			linkToSharedCode.Rows = append(linkToSharedCode.Rows, rowKey)
		} else {
			errors.Append(utils.PrefixError(
				fmt.Sprintf(`missing shared code %s`, rowKey.String()),
				fmt.Errorf(`referenced from %s`, transformation.String()),
			))
		}
	}

	return errors.ErrorOrNil()
}

// parseIdPlaceholder in transformation script.
func (m *remoteMapper) parseIdPlaceholder(code *model.Code, script model.Script, sharedCode *model.Config) (*model.ConfigRow, model.Script, error) {
	id := m.id.match(script.Content())
	if id == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code config row
	rowKey := model.ConfigRowKey{
		BranchId:    sharedCode.BranchId,
		ComponentId: sharedCode.ComponentId,
		ConfigId:    sharedCode.Id,
		Id:          id,
	}
	row, found := m.state.GetOrNil(rowKey).(*model.ConfigRow)
	if !found {
		return nil, nil, utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, rowKey.String()),
			fmt.Errorf(`referenced from %s`, code.String()),
		)
	}

	// Return LinkScript instead of ID
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}
