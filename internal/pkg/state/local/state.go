package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type objects = model.Objects

type State struct {
	objects
	fs           filesystem.Fs
	knownPaths   *knownpaths.Paths
	relatedPaths map[model.Key]*relatedpaths.Paths
	manifest     manifest.Manifest
	mapper       *mapper.Mapper
}

func NewState(sorter model.ObjectsSorter, fs filesystem.Fs, mapper *mapper.Mapper) (*State, error) {
	knownPaths, err := knownpaths.New(fs)
	if err != nil {
		return nil, err
	}

	return &State{
		objects:    object.NewCollection(sorter),
		fs:         fs,
		knownPaths: knownPaths,
		mapper:     mapper,
	}, nil
}

func (s *State) NewUnitOfWork(ctx context.Context, filter model.ObjectsFilter) state.UnitOfWork {
	backend := newUnitOfWorkBackend(ctx, s, filter, s.mapper)
	return state.NewUnitOfWork(ctx, s.objects, backend)
}

func (s *State) reloadKnownPaths() error {
	return nil
}

func (s *State) getRelatedPaths(key model.Key) *relatedpaths.Paths {
	v, _ := s.relatedPaths[key]
	return v
}

func (s *State) setRelatedPaths(key model.Key, v *relatedpaths.Paths) {
	s.relatedPaths[key] = v
}
