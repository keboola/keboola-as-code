package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *localMapper) getSharedCodeByPath(parentPath, codePath string) (*model.Config, error) {
	// Get key by path
	codePath = filesystem.Join(parentPath, codePath)
	configStateRaw, found := m.state.GetByPath(codePath)
	if !found {
		return nil, fmt.Errorf(`missing shared code "%s"`, codePath)
	}

	// Is config?
	configState, ok := configStateRaw.(*model.Config)
	if !ok {
		return nil, fmt.Errorf(`path "%s" is not shared code config`, codePath)
	}

	// Shared code?
	if configState.ComponentId != model.SharedCodeComponentId {
		return nil, fmt.Errorf(`config "%s" is not shared code`, codePath)
	}

	// Ok
	return configState, nil
}

func (h *localMapper) getSharedCodeRowByPath(sharedCode *model.Config, path string) (*model.ConfigRow, error) {
	// Get key by path
	path = filesystem.Join(sharedCode.Path(), path)
	configRowRaw, found := h.state.GetByPath(path)
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
		return nil, fmt.Errorf(`row "%s" is not from shared code "%s"`, path, sharedCode.Path())
	}

	// Ok
	return configRow, nil
}

// parsePathPlaceholder in transformation script.
func (m *localMapper) parsePathPlaceholder(code *model.Code, script model.Script, sharedCode *model.Config) (*model.ConfigRow, model.Script, error) {
	path := m.path.match(script.Content(), code.ComponentId)
	if path == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code row
	row, err := m.helper.getSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		return nil, nil, utils.PrefixError(
			err.Error(),
			fmt.Errorf(`referenced from "%s"`, code.String()),
		)
	}

	// Return LinkScript instead of path
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}

// parseIdPlaceholder in transformation script.
func (m *localMapper) parseIdPlaceholder(code *model.Code, script model.Script, sharedCode *model.Config) (*model.ConfigRow, model.Script, error) {
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
	row, found := m.state.GetOrNil(rowKey).(*model.ConfigRowState)
	if !found {
		return nil, nil, utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, rowKey.String()),
			fmt.Errorf(`referenced from %s`, code.String()),
		)
	}

	// Return LinkScript instead of ID
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}
