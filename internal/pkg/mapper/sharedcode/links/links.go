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

func (m *mapper) replaceIdByPathInScript(code *model.Code, script model.Script, sharedCode *model.ConfigState) (model.Script, error) {
	row, err := m.sharedCodeRowByScript(code, script, sharedCode.ConfigKey)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	path, err := filesystem.Rel(sharedCode.Path(), row.Path())
	if err != nil {
		return nil, err
	}

	// Return path instead of ID
	return model.StaticScript{Value: m.formatPath(path, code.ComponentId)}, nil
}

// replacePathByIdInScript from transformation code.
func (m *mapper) replacePathByIdInScript(code *model.Code, script model.Script, sharedCode *model.ConfigState) (*model.ConfigRowState, model.Script, error) {
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

	// Return ID instead of path
	return row, model.StaticScript{Value: m.formatId(row.Id)}, nil
}
