package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
		id:     newIdUtils(),
		path:   newPathUtils(),
	}
}

func (m *mapper) linkToIdPlaceholder(code *model.Code, link model.Script) (model.Script, error) {
	if link, ok := link.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.ConfigRowState)
		script := model.StaticScript{Value: m.id.format(row.Id)}
		if !ok {
			return script, utils.PrefixError(
				fmt.Sprintf(`missing shared code "%s"`, link.Target.Desc()),
				fmt.Errorf(`referenced from %s`, code.Path()),
			)
		}
		return script, nil
	}
	return nil, nil
}

func (m *mapper) linkToPathPlaceholder(code *model.Code, link model.Script, sharedCode *model.ConfigState) (model.Script, error) {
	if link, ok := link.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.ConfigRowState)
		if !ok || sharedCode == nil {
			// Return ID placeholder, if row is not found
			return model.StaticScript{Value: m.id.format(link.Target.Id)}, utils.PrefixError(
				fmt.Sprintf(`missing shared code %s`, link.Target.Desc()),
				fmt.Errorf(`referenced from %s`, code.Path()),
			)
		}

		path, err := filesystem.Rel(sharedCode.Path(), row.Path())
		if err != nil {
			return nil, err
		}
		return model.StaticScript{Value: m.path.format(path, code.ComponentId)}, nil
	}
	return nil, nil
}

// parseIdPlaceholder in transformation script.
func (m *mapper) parseIdPlaceholder(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
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
			fmt.Sprintf(`missing shared code %s`, rowKey.Desc()),
			fmt.Errorf(`referenced from %s`, code.Path()),
		)
	}

	// Return LinkScript instead of ID
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}

// parsePathPlaceholder in transformation script.
func (m *mapper) parsePathPlaceholder(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
	path := m.path.match(script.Content(), code.ComponentId)
	if path == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code row
	row, err := m.helper.GetSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		return nil, nil, utils.PrefixError(
			err.Error(),
			fmt.Errorf(`referenced from "%s"`, code.Path()),
		)
	}

	// Return LinkScript instead of path
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}
