package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/operation"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type objectsReadOnly = model.ObjectsReadOnly

type State struct {
	objectsReadOnly // objects can be read directly, bud modified only by UnitOfWOrk
	objects         model.Objects
	manager         *operation.Manager
	namingGenerator *naming.Generator
	mapper          *mapper.Mapper
}

func NewState(sorter model.ObjectsSorter, manager *operation.Manager) *State {
	objects := object.NewCollection(sorter)
	return &State{objectsReadOnly: objects, objects: objects, manager: manager}
}

func (s *State) ReloadPathsState() error {
	return nil
}

func (s *State) NewUnitOfWork(ctx context.Context, loadFilter model.ObjectsFilter) UnitOfWork {
	return newUnitOfWork(s, ctx, loadFilter)
}
