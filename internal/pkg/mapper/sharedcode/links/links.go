package links

import (
	"fmt"
	"regexp"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	IdFormat   = `{{<ID>}}`    // link to shared code used in API
	PathFormat = `{{:<PATH>}}` // link to shared code used locally
	IdRegexp   = `[0-9a-zA-Z_\-]+`
	PathRegexp = `[^:{}]+`
)

// mapper replaces "shared_code_id" with "shared_code_path" in local fs.
type mapper struct {
	model.MapperContext
	*helper.SharedCodeHelper
	localManager *local.Manager
	idRegexp     *regexp.Regexp
	pathRegexp   *regexp.Regexp
}

func NewMapper(localManager *local.Manager, context model.MapperContext) *mapper {
	return &mapper{
		MapperContext:    context,
		SharedCodeHelper: helper.New(context.State, context.Naming),
		localManager:     localManager,
		idRegexp:         idRegexp(),
		pathRegexp:       pathRegexp(),
	}
}

func (m *mapper) linkToIdPlaceholder(code *model.Code, link model.Script) (model.Script, error) {
	if link, ok := link.(model.LinkScript); ok {
		row, ok := m.State.GetOrNil(link.Target).(*model.ConfigRowState)
		script := model.StaticScript{Value: m.formatId(row.Id)}
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
		row, ok := m.State.GetOrNil(link.Target).(*model.ConfigRowState)
		if !ok || sharedCode == nil {
			// Return ID placeholder, if row is not found
			return model.StaticScript{Value: m.formatId(link.Target.Id)}, utils.PrefixError(
				fmt.Sprintf(`missing shared code %s`, link.Target.Desc()),
				fmt.Errorf(`referenced from %s`, code.Path()),
			)
		}

		path, err := filesystem.Rel(sharedCode.Path(), row.Path())
		if err != nil {
			return nil, err
		}
		return model.StaticScript{Value: m.formatPath(path, code.ComponentId)}, nil
	}
	return nil, nil
}

// parseIdPlaceholder in transformation script.
func (m *mapper) parseIdPlaceholder(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
	id := m.matchId(script.Content())
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
	row, found := m.State.GetOrNil(rowKey).(*model.ConfigRowState)
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
	path := m.matchPath(script.Content(), code.ComponentId)
	if path == "" {
		// Not found
		return nil, nil, nil
	}

	// Get shared code row
	row, err := m.GetSharedCodeRowByPath(sharedCode, path)
	if err != nil {
		return nil, nil, utils.PrefixError(
			err.Error(),
			fmt.Errorf(`referenced from "%s"`, code.Path()),
		)
	}

	// Return LinkScript instead of path
	return row, model.LinkScript{Target: row.ConfigRowKey}, nil
}
