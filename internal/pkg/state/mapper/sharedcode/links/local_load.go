package links

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// AfterLocalOperation - resolve shared codes paths, and replace them by IDs on local load.
func (m *localMapper) AfterLocalOperation(changes *model.Changes) error {
	errs := errors.NewMultiError()

	// Find loaded transformations
	for _, object := range changes.Loaded() {
		// Shared code can be used only by transformation - struct must be set
		if transformation, ok := object.(*model.Config); !ok || transformation.Transformation == nil {
			return nil
		} else {
			if err := m.onLocalLoad(transformation); err != nil {
				errs.Append(err)
			}
		}
	}

	return errs.ErrorOrNil()
}

// onLocalLoad replaces shared code path by id in transformation config and blocks.
func (m *localMapper) onLocalLoad(transformation *model.Config) error {
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
		return errors.PrefixError(
			fmt.Sprintf(`invalid transformation %s`, transformation.String()),
			fmt.Errorf(`key "%s" must be string, found %T`, model.SharedCodePathContentKey, sharedCodePathRaw),
		)
	}

	// Get shared code
	sharedCode, err := m.getSharedCodeByPath(transformation.BranchKey(), sharedCodePath)
	if err != nil {
		return errors.PrefixError(
			err.Error(),
			fmt.Errorf(`referenced from %s`, transformation.String()),
		)
	} else if sharedCode == nil {
		return errors.PrefixError(
			fmt.Sprintf(`missing shared code %s`, sharedCode.String()),
			fmt.Errorf(`referenced from %s`, transformation.String()),
		)
	}

	// Skip invalid shared code
	if sharedCode.SharedCode == nil {
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
	errs := errors.NewMultiError()
	foundSharedCodeRows := make(map[model.ConfigRowId]model.ConfigRowKey)
	transformation.Transformation.MapScripts(func(block *model.Block, code *model.Code, script model.Script) model.Script {
		if sharedCodeRow, v, err := m.parsePathPlaceholder(code, script, sharedCode); err != nil {
			codePath, err := m.state.GetPath(code)
			if err != nil {
				panic(err)
			}
			errs.AppendWithPrefix(err.Error(), fmt.Errorf(`referenced from "%s"`, codePath))
		} else if v != nil {
			foundSharedCodeRows[sharedCodeRow.ConfigRowId] = sharedCodeRow.ConfigRowKey
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
	return errs.ErrorOrNil()
}

// parsePathPlaceholder in transformation script.
func (m *localMapper) parsePathPlaceholder(code *model.Code, script model.Script, sharedCode *model.Config) (*model.ConfigRow, model.Script, error) {
	path := m.path.match(script.Content(), code.ComponentId())
	if path == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code row
	row, err := m.getSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		return nil, nil, err
	}

	// Return LinkScript instead of path
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}

func (m *localMapper) getSharedCodeRowByPath(sharedCode *model.Config, path string) (*model.ConfigRow, error) {
	sharedCodePath, err := m.state.GetPath(sharedCode)
	if err != nil {
		return nil, err
	}

	// Get key by path
	path = filesystem.Join(sharedCodePath.String(), path)
	configRowRaw, found := m.state.GetByPath(path)
	if !found {
		return nil, fmt.Errorf(`missing shared code "%s"`, path)
	}

	// Is config row?
	configRow, ok := configRowRaw.(*model.ConfigRow)
	if !ok {
		return nil, fmt.Errorf(`path "%s" is not config row`, path)
	}

	// Is from parent?
	if sharedCode.Key() != configRow.ConfigKey() {
		return nil, fmt.Errorf(`row "%s" is not from shared code "%s"`, path, sharedCodePath.String())
	}

	// Ok
	return configRow, nil
}
