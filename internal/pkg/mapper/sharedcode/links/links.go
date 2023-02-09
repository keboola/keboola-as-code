package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// mapper replaces "shared_code_id" with "shared_code_path" in local fs.
type mapper struct {
	state  *state.State
	logger log.Logger
	helper *helper.SharedCodeHelper
	id     *idUtils
	path   *pathUtils
}

func NewMapper(s *state.State) *mapper {
	return &mapper{
		state:  s,
		logger: s.Logger(),
		helper: helper.New(s),
		id:     newIDUtils(),
		path:   newPathUtils(),
	}
}

func (m *mapper) linkToIDPlaceholder(code *model.Code, link model.Script) (model.Script, error) {
	if link, ok := link.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.ConfigRowState)
		if !ok {
			return model.StaticScript{}, errors.NewNestedError(
				errors.Errorf(`missing shared code "%s"`, link.Target.Desc()),
				errors.Errorf(`referenced from %s`, code.Path()),
			)
		}
		return model.StaticScript{Value: m.id.format(row.ID)}, nil
	}
	return nil, nil
}

func (m *mapper) linkToPathPlaceholder(code *model.Code, link model.Script, sharedCode *model.ConfigState) (model.Script, error) {
	if link, ok := link.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.ConfigRowState)
		if !ok || sharedCode == nil {
			// Return ID placeholder, if row is not found
			return model.StaticScript{Value: m.id.format(link.Target.ID)}, errors.NewNestedError(
				errors.Errorf(`missing shared code %s`, link.Target.Desc()),
				errors.Errorf(`referenced from %s`, code.Path()),
			)
		}

		path, err := filesystem.Rel(sharedCode.Path(), row.Path())
		if err != nil {
			return nil, err
		}
		return model.StaticScript{Value: m.path.format(path, code.ComponentID)}, nil
	}
	return nil, nil
}

// parseIDPlaceholder in transformation script.
func (m *mapper) parseIDPlaceholder(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
	id := m.id.match(script.Content())
	if id == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code config row
	rowKey := model.ConfigRowKey{
		BranchID:    sharedCode.BranchID,
		ComponentID: sharedCode.ComponentID,
		ConfigID:    sharedCode.ID,
		ID:          id,
	}
	row, found := m.state.GetOrNil(rowKey).(*model.ConfigRowState)
	if !found {
		return nil, nil, errors.NewNestedError(
			errors.Errorf(`missing shared code %s`, rowKey.Desc()),
			errors.Errorf(`referenced from %s`, code.Path()),
		)
	}

	// Return LinkScript instead of ID
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}

// parsePathPlaceholder in transformation script.
func (m *mapper) parsePathPlaceholder(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
	path := m.path.match(script.Content(), code.ComponentID)
	if path == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code row
	row, err := m.helper.GetSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		return nil, nil, errors.NewNestedError(
			err,
			errors.Errorf(`referenced from "%s"`, code.Path()),
		)
	}

	// Return LinkScript instead of path
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}
