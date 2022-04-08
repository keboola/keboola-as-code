package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
)

type localDependencies interface {
	Components() (*model.ComponentsMap, error)
}

type variablesLocalMapper struct {
	localDependencies
	state *local.State
}

type variablesRemoteMapper struct {
	state *remote.State
}

func NewLocalMapper(s *local.State, d localDependencies) *variablesLocalMapper {
	return &variablesLocalMapper{state: s, localDependencies: d}
}

func NewRemoteMapper(s *remote.State) *variablesRemoteMapper {
	return &variablesRemoteMapper{state: s}
}
