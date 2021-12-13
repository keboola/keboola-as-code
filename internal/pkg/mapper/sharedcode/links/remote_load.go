package links

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// onRemoteLoad move shared code config/rows IDs from Content to Transformation struct.
func (m *mapper) onRemoteLoad(objectState model.ObjectState) error {
	// Shared code can be used only by transformation - struct must be set
	transformation, ok := objectState.RemoteState().(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

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
			fmt.Sprintf(`invalid transformation %s`, transformation.Desc()),
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
	sharedCodeState, found := m.State.GetOrNil(linkToSharedCode.Config).(*model.ConfigState)
	if !found || !sharedCodeState.HasRemoteState() {
		return utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, linkToSharedCode.Config.Desc()),
			fmt.Errorf(`referenced from %s`, objectState.Desc()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.CheckTargetComponent(sharedCodeState.LocalOrRemoteState().(*model.Config), transformation.ConfigKey); err != nil {
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
			fmt.Sprintf(`invalid transformation %s`, transformation.Desc()),
			fmt.Errorf(`key "%s" should be array, found %T`, model.SharedCodeRowsIdContentKey, sharedCodeRowsIdsRaw),
		)
	}

	errors := utils.NewMultiError()
	for _, rowId := range v {
		rowKey := model.ConfigRowKey{
			BranchId:    linkToSharedCode.Config.BranchId,
			ComponentId: linkToSharedCode.Config.ComponentId,
			ConfigId:    linkToSharedCode.Config.Id,
			Id:          model.RowId(cast.ToString(rowId)),
		}
		if _, found := m.State.Get(rowKey); found {
			linkToSharedCode.Rows = append(linkToSharedCode.Rows, rowKey)
		} else {
			errors.Append(utils.PrefixError(
				fmt.Sprintf(`missing shared code %s`, rowKey.Desc()),
				fmt.Errorf(`referenced from %s`, transformation.Desc()),
			))
		}
	}

	return errors.ErrorOrNil()
}
