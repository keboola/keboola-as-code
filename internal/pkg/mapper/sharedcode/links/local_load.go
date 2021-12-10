package links

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// onLocalLoad replaces shared code path by id in transformation config and blocks.
func (m *mapper) onLocalLoad(object model.Object) error {
	transformation, sharedCodePath, err := m.GetSharedCodePath(object)
	if err != nil {
		return err
	} else if transformation == nil {
		return nil
	}

	// Remove shared code id
	defer func() {
		transformation.Content.Delete(model.SharedCodePathContentKey)
	}()

	// Get shared code transformation
	sharedCodeState, err := m.GetSharedCodeByPath(transformation.BranchKey(), sharedCodePath)
	if err != nil {
		errors := utils.NewMultiError()
		errors.Append(err)
		errors.Append(fmt.Errorf(`  - referenced from %s`, transformation.Desc()))
		return errors
	}
	sharedCode := sharedCodeState.LocalOrRemoteState().(*model.Config)
	targetComponentId, err := m.GetTargetComponentId(sharedCode)
	if err != nil {
		return err
	}

	// Check componentId
	if targetComponentId != transformation.ComponentId {
		errors := utils.NewMultiError()
		errors.Append(fmt.Errorf(`unexpected shared code "%s" in %s`, model.ShareCodeTargetComponentKey, sharedCodeState.Desc()))
		errors.Append(fmt.Errorf(`  - expected "%s"`, transformation.ComponentId))
		errors.Append(fmt.Errorf(`  - found "%s"`, targetComponentId))
		errors.Append(fmt.Errorf(`  - referenced from %s`, transformation.Desc()))
		return errors
	}

	// Replace Shared Code Path -> Shared Code ID
	transformation.Content.Set(model.SharedCodeIdContentKey, sharedCodeState.Id.String())

	// Replace paths -> IDs in scripts
	errors := utils.NewMultiError()
	rowIdsMap := make(map[model.RowId]bool)
	for _, block := range transformation.Transformation.Blocks {
		for _, code := range block.Codes {
			for index, script := range code.Scripts {
				if id, v, err := m.replacePathByIdInScript(script, code, sharedCodeState); err != nil {
					errors.Append(err)
					continue
				} else if v != "" {
					rowIdsMap[id] = true
					code.Scripts[index] = v
				}
			}
		}
	}

	// Convert row IDs map -> slice
	rowIds := make([]interface{}, 0)
	for id := range rowIdsMap {
		rowIds = append(rowIds, id.String())
	}
	sort.SliceStable(rowIds, func(i, j int) bool {
		return rowIds[i].(string) < rowIds[j].(string)
	})

	// Set rows IDs
	transformation.Content.Set(model.SharedCodeRowsIdContentKey, rowIds)
	return errors.ErrorOrNil()
}

// replacePathByIdInScript from transformation code.
func (m *mapper) replacePathByIdInScript(script string, code *model.Code, sharedCode *model.ConfigState) (model.RowId, string, error) {
	path := m.matchPath(script, code.ComponentId)
	if path == "" {
		// Not found
		return "", "", nil
	}

	// Get shared code config row
	row, err := m.GetSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		errors := utils.NewMultiError()
		errors.Append(err)
		errors.Append(fmt.Errorf(`  - referenced from "%s"`, code.Path()))
		return "", "", errors
	}

	// Return ID instead of path
	return row.Id, m.formatId(row.Id), nil
}
