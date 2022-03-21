package remote

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type objects = model.Objects

type State struct {
	objects
	api    *storageapi.Api
	mapper *mapper.Mapper
}

func NewState(sorter model.ObjectsSorter, mapper *mapper.Mapper, api *storageapi.Api) *State {
	return &State{objects: object.NewCollection(sorter), api: api, mapper: mapper}
}

func (s *State) NewUnitOfWork(ctx context.Context, changeDescription string, loadFilter model.ObjectsFilter) UnitOfWork {
	return newUnitOfWork(s, ctx, changeDescription, loadFilter)
}
