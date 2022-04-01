// Package links replaces "shared_code_id" with "shared_code_path" in local fs and vice versa.
package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/sharedcode/helper"
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
