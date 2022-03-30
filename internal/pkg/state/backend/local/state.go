package local

import (
	"context"
	"sort"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
)

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

type objects = model.Objects

type State struct {
	objects
	deps            dependencies
	logger          log.Logger
	objectsRoot     filesystem.Fs
	manifest        manifest.Manifest
	namingGenerator *naming.Generator
	mapper          *mapper.Mapper
	knownPaths      *knownpaths.Paths

	lock            *sync.Mutex
	relatedPaths    map[model.Key]*relatedpaths.Paths
	notFoundObjects map[model.Key]model.ObjectManifest
	invalidObjects  map[model.Key]model.Object
}

type MappersFactory func(*State) (mapper.Mappers, error)

func NewState(d dependencies, objectsRoot filesystem.Fs, manifest manifest.Manifest, mappersFn MappersFactory) (*State, error) {
	components, err := d.Components()
	if err != nil {
		return nil, err
	}

	knownPaths, err := knownpaths.New(objectsRoot)
	if err != nil {
		return nil, err
	}

	// Create state
	s := &State{
		objects:         state.NewCollection(manifest.Sorter()),
		deps:            d,
		logger:          d.Logger(),
		objectsRoot:     objectsRoot,
		namingGenerator: naming.NewGenerator(manifest.NamingTemplate(), manifest.NamingRegistry(), components),
		manifest:        manifest,
		mapper:          mapper.New(),
		knownPaths:      knownPaths,
		lock:            &sync.Mutex{},
		relatedPaths:    make(map[model.Key]*relatedpaths.Paths),
		notFoundObjects: make(map[model.Key]model.ObjectManifest),
		invalidObjects:  make(map[model.Key]model.Object),
	}

	// Create mappers
	if mappersFn != nil {
		mappers, err := mappersFn(s)
		if err != nil {
			return nil, err
		}
		s.mapper.AddMapper(mappers...)
	}

	return s, nil
}

func (s *State) NewUnitOfWork(ctx context.Context, filter model.ObjectsFilter) state.UnitOfWork {
	backend := newUnitOfWorkBackend(s, ctx, filter, s.mapper)
	return state.NewUnitOfWork(ctx, s.objects, backend)
}

func (s *State) Ctx() context.Context {
	return s.deps.Ctx()
}

func (s *State) Manifest() manifest.Manifest {
	return s.manifest
}

func (s *State) ObjectsRoot() filesystem.Fs {
	return s.objectsRoot
}

func (s *State) Mapper() *mapper.Mapper {
	return s.mapper
}

func (s *State) NamingRegistry() *naming.Registry {
	return s.manifest.NamingRegistry()
}

func (s *State) NamingGenerator() *naming.Generator {
	return s.namingGenerator
}

func (s *State) FileLoader() filesystem.FileLoader {
	return s.mapper.NewFileLoader(s.objectsRoot)
}

func (s *State) TrackedPaths() []string {
	return s.knownPaths.TrackedPaths()
}

func (s *State) UntrackedPaths() []string {
	return s.knownPaths.UntrackedPaths()
}

func (s *State) GetByPath(path string) (model.Object, bool) {
	key, found := s.manifest.NamingRegistry().KeyByPath(path)
	if !found {
		return nil, false
	}
	return s.Get(key)
}

func (s *State) GetPath(object model.WithKey) (model.AbsPath, error) {
	return s.namingGenerator.GetOrGenerate(object)
}

func (s *State) GetRelatedPaths(object model.WithKey) (*relatedpaths.Paths, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := object.Key()
	if _, found := s.relatedPaths[key]; !found {
		if path, err := s.GetPath(object); err == nil {
			s.relatedPaths[key] = relatedpaths.New(path)
		} else {
			return nil, err
		}
	}
	return s.relatedPaths[key], nil
}

func (s *State) GetRelatedPathsByKey(key model.Key) (*relatedpaths.Paths, bool) {
	v, found := s.relatedPaths[key]
	return v, found
}

func (s *State) SetRelatedPaths(key model.Key, v *relatedpaths.Paths) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.relatedPaths[key] = v
}

func (s *State) InvalidObjects() []model.Key {
	s.lock.Lock()
	defer s.lock.Unlock()

	i := 0
	out := make([]model.Key, len(s.invalidObjects))
	for _, v := range s.invalidObjects {
		out[i] = v.Key()
		i++
	}
	sort.SliceStable(out, func(i, j int) bool {
		return s.Less(out[i], out[j])
	})
	return out
}

func (s *State) NotFoundObjects() []model.Key {
	s.lock.Lock()
	defer s.lock.Unlock()

	i := 0
	out := make([]model.Key, len(s.notFoundObjects))
	for _, v := range s.notFoundObjects {
		out[i] = v.Key()
		i++
	}
	sort.SliceStable(out, func(i, j int) bool {
		return s.Less(out[i], out[j])
	})
	return out
}

func (s *State) reloadKnownPaths() error {
	return nil
}

func (s *State) addNotFoundObject(objectManifest model.ObjectManifest) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.notFoundObjects[objectManifest.Key()] = objectManifest
}

func (s *State) addInvalidObject(object model.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.invalidObjects[object.Key()] = object
}
