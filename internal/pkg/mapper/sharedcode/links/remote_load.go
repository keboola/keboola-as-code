package links

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
		transformation.Content.Delete(model.SharedCodeIDContentKey)
		transformation.Content.Delete(model.SharedCodeRowsIDContentKey)
	}()

	// Get shared code ID
	sharedCodeIDRaw, found := transformation.Content.Get(model.SharedCodeIDContentKey)
	sharedCodeID, ok := sharedCodeIDRaw.(string)
	if !found {
		return nil
	} else if !ok {
		return errors.NewNestedError(
			errors.Errorf(`invalid transformation %s`, transformation.Desc()),
			errors.Errorf(`key "%s" should be string, found %T`, model.SharedCodeIDContentKey, sharedCodeIDRaw),
		)
	}

	// Get shared code
	linkToSharedCode := &model.LinkToSharedCode{
		Config: model.ConfigKey{
			BranchID:    transformation.BranchID,
			ComponentID: keboola.SharedCodeComponentID,
			ID:          keboola.ConfigID(sharedCodeID),
		},
	}
	sharedCodeState, found := m.state.GetOrNil(linkToSharedCode.Config).(*model.ConfigState)
	if !found || !sharedCodeState.HasRemoteState() {
		return errors.NewNestedError(
			errors.Errorf(`missing shared code %s`, linkToSharedCode.Config.Desc()),
			errors.Errorf(`referenced from %s`, objectState.Desc()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCodeState.LocalOrRemoteState().(*model.Config), transformation.ConfigKey); err != nil {
		return err
	}

	// Store shared code config key in Transformation structure
	transformation.Transformation.LinkToSharedCode = linkToSharedCode

	// Get shared code config rows IDs
	sharedCodeRowsIdsRaw, found := transformation.Content.Get(model.SharedCodeRowsIDContentKey)
	v, ok := sharedCodeRowsIdsRaw.([]any)
	if !found {
		return nil
	} else if !ok {
		return errors.NewNestedError(
			errors.Errorf(`invalid transformation %s`, transformation.Desc()),
			errors.Errorf(`key "%s" should be array, found %T`, model.SharedCodeRowsIDContentKey, sharedCodeRowsIdsRaw),
		)
	}

	// Replace ID placeholder with LinkScript struct
	errs := errors.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		if _, v, err := m.parseIDPlaceholder(code, script, sharedCodeState); err != nil {
			errs.Append(err)
		} else if v != nil {
			return v
		}
		return script
	})

	// Check rows IDs
	for _, rowID := range v {
		rowKey := model.ConfigRowKey{
			BranchID:    linkToSharedCode.Config.BranchID,
			ComponentID: linkToSharedCode.Config.ComponentID,
			ConfigID:    linkToSharedCode.Config.ID,
			ID:          keboola.RowID(cast.ToString(rowID)),
		}
		if _, found := m.state.Get(rowKey); found {
			linkToSharedCode.Rows = append(linkToSharedCode.Rows, rowKey)
		} else {
			errs.Append(errors.NewNestedError(
				errors.Errorf(`missing shared code %s`, rowKey.Desc()),
				errors.Errorf(`referenced from %s`, transformation.Desc()),
			))
		}
	}

	return errs.ErrorOrNil()
}
