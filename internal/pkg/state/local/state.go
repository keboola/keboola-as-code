package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/operation"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type objects = model.Objects

type State struct {
	objects
	manager *operation.Manager
}

func NewState(sorter model.ObjectsSorter, manager *operation.Manager) *State {
	return &State{objects: object.NewCollection(sorter), manager: manager}
}

func (s *State) ReloadPathsState() error {
	return nil
}

func (s *State) NewUnitOfWork(ctx context.Context, loadFilter model.ObjectsFilter) UnitOfWork {
	return newUnitOfWork(s, ctx, loadFilter)
}
