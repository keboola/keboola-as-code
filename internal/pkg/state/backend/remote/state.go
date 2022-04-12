package remote

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
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
	mapper     *Mapper
}

type MappersFactory func(*State) (Mappers, error)

func NewState(d dependencies, sorter model.ObjectsSorter, mappersFn MappersFactory) (*State, error) {
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// Create state
	s := &State{
		objects:    state.NewCollection(state.WithSorter(sorter)),
		deps:       d,
		logger:     d.Logger(),
		storageApi: storageApi,
	}

	// Create mappers
	s.mapper = NewMapper(s)
	if mappersFn != nil {
		mappers, err := mappersFn(s)
		if err != nil {
			return nil, err
		}
		s.mapper.AddMapper(mappers...)
	}

	return s, nil
}

func (s *State) NewUnitOfWork(ctx context.Context, changeDescription string) state.UnitOfWork {
	backend := newUnitOfWorkBackend(s, ctx, changeDescription)
	return state.NewUnitOfWork(ctx, s.objects, backend)
}

func (s *State) Mapper() *Mapper {
	return s.mapper
}
