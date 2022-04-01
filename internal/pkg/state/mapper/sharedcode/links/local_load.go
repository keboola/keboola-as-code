package links

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// AfterLocalOperation - resolve shared codes paths, and replace them by IDs on local load.
func (m *localMapper) AfterLocalOperation(changes *model.Changes) error {
	// Process loaded objects
	errors := utils.NewMultiError()
	for _, object := range changes.Loaded() {
		if err := m.onLocalLoad(object); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

// onLocalLoad replaces shared code path by id in transformation config and blocks.
func (m *localMapper) onLocalLoad(object model.Object) error {
	// Shared code can be used only by transformation - struct must be set
	transformation, ok := object.(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

	// Always remove shared code path from Content
	defer func() {
		transformation.Content.Delete(model.SharedCodePathContentKey)
	}()

	// Get shared code path
	sharedCodePathRaw, found := transformation.Content.Get(model.SharedCodePathContentKey)
	sharedCodePath, ok := sharedCodePathRaw.(string)
	if !found {
		return nil
	} else if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`invalid transformation %s`, transformation.String()),
			fmt.Errorf(`key "%s" must be string, found %T`, model.SharedCodePathContentKey, sharedCodePathRaw),
		)
	}

	// Get shared code
	sharedCode, err := m.getSharedCodeByPath(recipe.PAth, sharedCodePath)
	if err != nil {
		return utils.PrefixError(
			err.Error(),
			fmt.Errorf(`referenced from %s`, object.String()),
		)
	} else if sharedCode == nil {
		return utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, sharedCode.String()),
			fmt.Errorf(`referenced from %s`, object.String()),
		)
	}
	if sharedCode.SharedCode == nil {
		// Value is not set, shared code is not valid -> skip
		return nil
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCode, transformation.ConfigKey); err != nil {
		return err
	}

	// Store shared code config key in Transformation structure
	linkToSharedCode := &model.LinkToSharedCode{Config: sharedCode.ConfigKey}
	transformation.Transformation.LinkToSharedCode = linkToSharedCode

	// Replace paths -> IDs in code scripts
	errors := utils.NewMultiError()
	foundSharedCodeRows := make(map[model.RowId]model.ConfigRowKey)
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		if sharedCodeRow, v, err := m.parsePathPlaceholder(code, script, sharedCode); err != nil {
			errors.Append(err)
		} else if v != nil {
			foundSharedCodeRows[sharedCodeRow.Id] = sharedCodeRow.ConfigRowKey
			return v
		}
		return script
	})

	// Sort rows IDs
	for _, rowKey := range foundSharedCodeRows {
		linkToSharedCode.Rows = append(linkToSharedCode.Rows, rowKey)
	}
	sort.SliceStable(linkToSharedCode.Rows, func(i, j int) bool {
		return linkToSharedCode.Rows[i].String() < linkToSharedCode.Rows[j].String()
	})
	return errors.ErrorOrNil()
}
