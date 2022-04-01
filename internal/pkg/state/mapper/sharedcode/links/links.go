// Package links replaces "shared_code_id" with "shared_code_path" in local fs and vice versa.
package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

type localMapper struct {
	state  *local.State
	logger log.Logger
	helper *helper.SharedCodeHelper
	id     *idUtils
	path   *pathUtils
}

type remoteMapper struct {
	state  *remote.State
	logger log.Logger
	helper *helper.SharedCodeHelper
	id     *idUtils
	path   *pathUtils
}

func NewLocalMapper(s *local.State, d dependencies) *localMapper {
	return &localMapper{
		state:  s,
		logger: d.Logger(),
		helper: helper.New(s, d),
		id:     newIdUtils(),
		path:   newPathUtils(),
	}
}

func NewRemoteMapper(s *remote.State, d dependencies) *remoteMapper {
	return &remoteMapper{
		state:  s,
		logger: d.Logger(),
		helper: helper.New(s, d),
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
				fmt.Sprintf(`missing shared code "%s"`, link.Target.String()),
				fmt.Errorf(`referenced from %s`, code.String()),
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
				fmt.Sprintf(`missing shared code %s`, link.Target.String()),
				fmt.Errorf(`referenced from %s`, code.String()),
			)
		}

		path, err := filesystem.Rel(sharedCode.Path(), row.Path())
		if err != nil {
			return nil, err
		}
		return model.StaticScript{Value: m.path.format(path, code.ComponentId)}, nil
	}
	return nil, nil
}y
