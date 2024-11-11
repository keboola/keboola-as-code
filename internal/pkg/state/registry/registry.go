package registry

import (
	"context"
	"sync"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pathsRO = knownpaths.PathsReadOnly

const KBCIgnoreFilePath = ".keboola/kbc_ignore"

type Registry struct {
	*pathsRO
	paths          *knownpaths.Paths
	sortBy         string
	lock           *sync.Mutex
	namingRegistry *naming.Registry
	components     *ComponentsMap
	objects        *orderedmap.OrderedMap
}

func New(paths *knownpaths.Paths, namingRegistry *naming.Registry, components *ComponentsMap, sortBy string) *Registry {
	return &Registry{
		pathsRO:        paths.ReadOnly(),
		paths:          paths,
		sortBy:         sortBy,
		lock:           &sync.Mutex{},
		namingRegistry: namingRegistry,
		components:     components,
		objects:        orderedmap.New(),
	}
}

func (s *Registry) Components() *ComponentsMap {
	return s.components
}

func (s *Registry) PathsState() *knownpaths.Paths {
	return s.paths.Clone()
}

func (s *Registry) ReloadPathsState(ctx context.Context) error {
	// Create a new paths state -> all paths are untracked
	if err := s.paths.Reset(ctx); err != nil {
		return errors.Errorf(`cannot reload paths state: %w`, err)
	}

	// Track all known paths
	for _, object := range s.All() {
		s.TrackObjectPaths(object.Manifest())
	}
	return nil
}

func (s *Registry) TrackObjectPaths(manifest ObjectManifest) {
	if !manifest.State().IsPersisted() {
		return
	}

	// Track object path
	s.paths.MarkTracked(manifest.Path())

	// Track sub-paths
	if manifest.State().IsInvalid() {
		// Object is invalid, no sub-paths has been parsed -> mark all sub-paths tracked.
		s.paths.MarkSubPathsTracked(manifest.Path())
	} else {
		// Object is valid, track loaded files.
		for _, path := range manifest.GetRelatedPaths() {
			s.paths.MarkTracked(path)
		}
	}
}

func (s *Registry) All() []ObjectState {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.objects.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value.(ObjectState).Manifest().SortKey(s.sortBy)
		bKey := b.Value.(ObjectState).Manifest().SortKey(s.sortBy)
		return aKey < bKey
	})

	var out []ObjectState
	for _, key := range s.objects.Keys() {
		// Get value
		v, _ := s.objects.Get(key)
		object := v.(ObjectState)

		// Skip deleted
		if !object.HasLocalState() && !object.HasRemoteState() {
			continue
		}

		out = append(out, object)
	}

	return out
}

func (s *Registry) ObjectsInState(stateType StateType) Objects {
	switch stateType {
	case StateTypeRemote:
		return s.RemoteObjects()
	case StateTypeLocal:
		return s.LocalObjects()
	default:
		panic(errors.Errorf(`unexpected StateType "%v"`, stateType))
	}
}

func (s *Registry) LocalObjects() Objects {
	return NewProxy(s, StateTypeLocal)
}

func (s *Registry) RemoteObjects() Objects {
	return NewProxy(s, StateTypeRemote)
}

func (s *Registry) MainBranch() *BranchState {
	for _, b := range s.Branches() {
		if b.LocalOrRemoteState().(*Branch).IsDefault {
			return b
		}
	}
	panic(errors.New("no default branch found"))
}

func (s *Registry) Branches() (branches []*BranchState) {
	for _, object := range s.All() {
		if v, ok := object.(*BranchState); ok {
			branches = append(branches, v)
		}
	}
	return branches
}

func (s *Registry) Configs() (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *Registry) IgnoreConfig(ignoreID string, componentID string) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			if v.ID.String() == ignoreID && v.ComponentID.String() == componentID {
				// ignore configuration
				v.Ignore = true

				// ignore rows of the configuration
				if len(s.ConfigRowsFrom(v.ConfigKey)) > 0 {
					for _, configRowState := range s.ConfigRowsFrom(v.ConfigKey) {
						configRowState.Ignore = true
					}
				}
			}
		}
	}
}

func (s *Registry) IgnoredConfigs() (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			if v.Ignore {
				configs = append(configs, v)
			}
		}
	}
	return configs
}

func (s *Registry) ConfigsFrom(branch BranchKey) (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			if v.BranchID != branch.ID {
				continue
			}
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *Registry) ConfigRows() (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *Registry) IgnoreConfigRow(configID, rowID string) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			if v.ConfigID.String() == configID && v.ID.String() == rowID {
				v.Ignore = true
			}
		}
	}
}

func (s *Registry) IgnoredConfigRows() (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			if v.Ignore {
				rows = append(rows, v)
			}
		}
	}
	return rows
}

func (s *Registry) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			if v.BranchID != config.BranchID || v.ComponentID != config.ComponentID || v.ConfigID != config.ID {
				continue
			}
			rows = append(rows, v)
		}
	}
	return rows
}

// func (s *Registry) SetIgnoredConfigsOrRows(ctx context.Context, fs filesystem.Fs, path string) error {
//	//strukturu pattre.field
//	content, err := fs.ReadFile(ctx, filesystem.NewFileDef(path))
//	if err != nil {
//		return err
//	}
//
//	if content.Content == "" {
//		return nil
//	}
//
//	return s.applyIgnoredPatterns(content.Content)
//}

func (s *Registry) GetPath(key Key) (AbsPath, bool) {
	objectState, found := s.Get(key)
	if !found {
		return AbsPath{}, false
	}
	return objectState.GetAbsPath(), true
}

func (s *Registry) GetByPath(path string) (ObjectState, bool) {
	key, found := s.namingRegistry.KeyByPath(path)
	if !found {
		return nil, false
	}
	return s.Get(key)
}

func (s *Registry) Get(key Key) (ObjectState, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if v, ok := s.objects.Get(key.String()); ok {
		return v.(ObjectState), true
	}
	return nil, false
}

func (s *Registry) GetOrNil(key Key) ObjectState {
	v, _ := s.Get(key)
	return v
}

func (s *Registry) MustGet(key Key) ObjectState {
	state, found := s.Get(key)
	if !found {
		panic(errors.Errorf(`%s not found`, key.Desc()))
	}
	return state
}

func (s *Registry) CreateFrom(manifest ObjectManifest) (ObjectState, error) {
	objectState := manifest.NewObjectState()
	return objectState, s.Set(objectState)
}

func (s *Registry) Set(objectState ObjectState) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	key := objectState.Key()
	if _, found := s.objects.Get(key.String()); found {
		return errors.Errorf(`object "%s" already exists`, key.Desc())
	}

	if objectState.GetRelativePath() != "" {
		if err := s.namingRegistry.Attach(key, objectState.GetAbsPath()); err != nil {
			return err
		}
	}

	s.objects.Set(key.String(), objectState)
	return nil
}

func (s *Registry) Remove(key Key) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.objects.Delete(key.String())
	s.namingRegistry.Detach(key)
}

func (s *Registry) GetOrCreateFrom(manifest ObjectManifest) (ObjectState, error) {
	if objectState, found := s.Get(manifest.Key()); found {
		objectState.SetManifest(manifest)
		return objectState, nil
	}

	return s.CreateFrom(manifest)
}
