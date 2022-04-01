package codes

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
	*helper.SharedCodeHelper
	state  *local.State
	logger log.Logger
}

type remoteMapper struct {
	*helper.SharedCodeHelper
	state  *remote.State
	logger log.Logger
}

func NewLocalMapper(s *local.State, d dependencies) *localMapper {
	return &localMapper{state: s, logger: d.Logger(), SharedCodeHelper: helper.New(s, d)}
}

func NewRemoteMapper(s *remote.State, d dependencies) *remoteMapper {
	return &remoteMapper{state: s, logger: d.Logger(), SharedCodeHelper: helper.New(s, d)}
}
