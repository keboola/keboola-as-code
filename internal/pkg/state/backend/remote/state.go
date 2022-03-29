package remote

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type dependencies interface {
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
}

type objects = model.Objects

type State struct {
	objects
	deps       dependencies
	logger     log.Logger
	storageApi *storageapi.Api
	mapper     *mapper.Mapper
}

type MappersFactory func(*State) (mapper.Mappers, error)

func NewState(d dependencies, sorter model.ObjectsSorter, mappersFn MappersFactory) (*State, error) {
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// Create state
	s := &State{
		objects:    object.NewCollection(sorter),
		deps:       d,
		logger:     d.Logger(),
		storageApi: storageApi,
		mapper:     mapper.New(),
	}

	// Create mappers
	mappers, err := mappersFn(s)
	if err != nil {
		return nil, err
	}

	// Set mappers
	s.mapper.AddMapper(mappers...)
	return s, nil
}

func (s *State) NewUnitOfWork(ctx context.Context, filter model.ObjectsFilter, changeDescription string) state.UnitOfWork {
	backend := newUnitOfWorkBackend(s, ctx, filter, changeDescription)
	return state.NewUnitOfWork(ctx, s.objects, backend)
}
