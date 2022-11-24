package links

import (
	"github.com/keboola/go-client/pkg/storageapi"
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
		transformation.Content.Delete(model.SharedCodeIdContentKey)
		transformation.Content.Delete(model.SharedCodeRowsIdContentKey)
	}()

	// Get shared code ID
	sharedCodeIdRaw, found := transformation.Content.Get(model.SharedCodeIdContentKey)
	sharedCodeId, ok := sharedCodeIdRaw.(string)
	if !found {
		return nil
	} else if !ok {
		errs := errors.NewNestedError(
			errors.Errorf(`invalid transformation %s`, transformation.Desc()),
			errors.Errorf(`key "%s" should be string, found %T`, model.SharedCodeIdContentKey, sharedCodeIdRaw),
		)
		m.logger.Warn(errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
		return nil
	}

	// Get shared code
	linkToSharedCode := &model.LinkToSharedCode{
		Config: model.ConfigKey{
			BranchId:    transformation.BranchId,
			ComponentId: storageapi.SharedCodeComponentID,
			Id:          storageapi.ConfigID(sharedCodeId),
		},
	}
	sharedCodeState, found := m.state.GetOrNil(linkToSharedCode.Config).(*model.ConfigState)
	if !found || !sharedCodeState.HasRemoteState() {
		errs := errors.NewNestedError(
			errors.Errorf(`missing shared code %s`, linkToSharedCode.Config.Desc()),
			errors.Errorf(`referenced from %s`, objectState.Desc()),
		)
		m.logger.Warn(errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
		return nil
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCodeState.LocalOrRemoteState().(*model.Config), transformation.ConfigKey); err != nil {
		m.logger.Warn(errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
		return nil
	}

	// Store shared code config key in Transformation structure
	transformation.Transformation.LinkToSharedCode = linkToSharedCode

	// Get shared code config rows IDs
	sharedCodeRowsIdsRaw, found := transformation.Content.Get(model.SharedCodeRowsIdContentKey)
	v, ok := sharedCodeRowsIdsRaw.([]interface{})
	if !found {
		return nil
	} else if !ok {
		errs := errors.NewNestedError(
			errors.Errorf(`invalid transformation %s`, transformation.Desc()),
			errors.Errorf(`key "%s" should be array, found %T`, model.SharedCodeRowsIdContentKey, sharedCodeRowsIdsRaw),
		)
		m.logger.Warn(errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
		return nil
	}

	// Replace ID placeholder with LinkScript struct
	errs := errors.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		if _, v, err := m.parseIdPlaceholder(code, script, sharedCodeState); err != nil {
			errs.Append(err)
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
			Id:          storageapi.RowID(cast.ToString(rowId)),
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

	if errs.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
	}
	return nil
}
