package registry

import (
	"context"
	"sync"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pathsRO = knownpaths.PathsReadOnly

type Registry struct {
	*pathsRO
	paths          *knownpaths.Paths
	sortBy         string
	lock           *sync.Mutex
	namingRegistry *naming.Registry
	components     *model.ComponentsMap
	objects        *orderedmap.OrderedMap
}

func New(paths *knownpaths.Paths, namingRegistry *naming.Registry, components *model.ComponentsMap, sortBy string) *Registry {
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

func (s *Registry) Components() *model.ComponentsMap {
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

func (s *Registry) TrackObjectPaths(manifest model.ObjectManifest) {
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

func (s *Registry) All() []model.ObjectState {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.objects.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value.(model.ObjectState).Manifest().SortKey(s.sortBy)
		bKey := b.Value.(model.ObjectState).Manifest().SortKey(s.sortBy)
		return aKey < bKey
	})

	out := make([]model.ObjectState, 0, len(s.objects.Keys()))
	for _, key := range s.objects.Keys() {
		// Get value
		v, _ := s.objects.Get(key)
		object := v.(model.ObjectState)

		// Skip deleted
		if !object.HasLocalState() && !object.HasRemoteState() {
			continue
		}

		out = append(out, object)
	}

	return out
}

func (s *Registry) ObjectsInState(stateType model.StateType) model.Objects {
	switch stateType {
	case model.StateTypeRemote:
		return s.RemoteObjects()
	case model.StateTypeLocal:
		return s.LocalObjects()
	default:
		panic(errors.Errorf(`unexpected StateType "%v"`, stateType))
	}
}

func (s *Registry) LocalObjects() model.Objects {
	return NewProxy(s, model.StateTypeLocal)
}

func (s *Registry) RemoteObjects() model.Objects {
	return NewProxy(s, model.StateTypeRemote)
}

func (s *Registry) MainBranch() *model.BranchState {
	for _, b := range s.Branches() {
		if b.LocalOrRemoteState().(*model.Branch).IsDefault {
			return b
		}
	}
	panic(errors.New("no default branch found"))
}

func (s *Registry) Branches() (branches []*model.BranchState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.BranchState); ok {
			branches = append(branches, v)
		}
	}
	return branches
}

func (s *Registry) Configs() (configs []*model.ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigState); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *Registry) IgnoreBranch(branchName string) {
	for _, branch := range s.Branches() {
		if branch.LocalOrRemoteState().(*model.Branch).Name == branchName {
			branch.Ignore = true
		}
	}
}

func (s *Registry) IgnoredBranches() (branches []*model.BranchState) {
	for _, branch := range s.Branches() {
		if branch.Ignore {
			branches = append(branches, branch)
		}
	}
	return branches
}

// NullIgnoredBranchStates nulls local+remote state for every ignored branch and for all
// configs/rows belonging to those branches, using a single pass over All() to avoid
// repeated sorts.
func (s *Registry) NullIgnoredBranchStates() {
	ignored := s.IgnoredBranches()
	if len(ignored) == 0 {
		return
	}

	// Build set of ignored branch keys for O(1) lookup.
	ignoredKeys := make(map[model.BranchKey]struct{}, len(ignored))
	for _, b := range ignored {
		ignoredKeys[b.BranchKey] = struct{}{}
	}

	for _, obj := range s.All() {
		switch v := obj.(type) {
		case *model.BranchState:
			if _, ok := ignoredKeys[v.BranchKey]; ok {
				v.SetLocalState(nil)
				v.SetRemoteState(nil)
			}
		case *model.ConfigState:
			if _, ok := ignoredKeys[model.BranchKey{ID: v.BranchID}]; ok {
				v.SetLocalState(nil)
				v.SetRemoteState(nil)
			}
		case *model.ConfigRowState:
			if _, ok := ignoredKeys[model.BranchKey{ID: v.BranchID}]; ok {
				v.SetLocalState(nil)
				v.SetRemoteState(nil)
			}
		}
	}
}

func (s *Registry) IgnoreConfig(ignoreID string, componentID string) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigState); ok {
			if v.ID.String() == ignoreID && v.ComponentID.String() == componentID {
				// ignore configuration
				v.Ignore = true

				// ignore rows of the configuration
				for _, configRowState := range s.ConfigRowsFrom(v.ConfigKey) {
					configRowState.Ignore = true
				}
			}
		}
	}
}

func (s *Registry) IgnoredConfigs() (configs []*model.ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigState); ok {
			if v.Ignore {
				configs = append(configs, v)
			}
		}
	}
	return configs
}

func (s *Registry) ConfigsFrom(branch model.BranchKey) (configs []*model.ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigState); ok {
			if v.BranchID != branch.ID {
				continue
			}
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *Registry) ConfigRows() (rows []*model.ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigRowState); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *Registry) IgnoreConfigRow(configID, rowID string) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigRowState); ok {
			if v.ConfigID.String() == configID && v.ID.String() == rowID {
				v.Ignore = true
			}
		}
	}
}

func (s *Registry) IgnoredConfigRows() (rows []*model.ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigRowState); ok {
			if v.Ignore {
				rows = append(rows, v)
			}
		}
	}
	return rows
}

func (s *Registry) ConfigRowsFrom(config model.ConfigKey) (rows []*model.ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigRowState); ok {
			if v.BranchID != config.BranchID || v.ComponentID != config.ComponentID || v.ConfigID != config.ID {
				continue
			}
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *Registry) Notifications() (notifications []*model.NotificationState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.NotificationState); ok {
			notifications = append(notifications, v)
		}
	}
	return notifications
}

func (s *Registry) NotificationsFrom(config model.ConfigKey) (notifications []*model.NotificationState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.NotificationState); ok {
			if v.BranchID != config.BranchID || v.ComponentID != config.ComponentID || v.ConfigID != config.ID {
				continue
			}
			notifications = append(notifications, v)
		}
	}
	return notifications
}

func (s *Registry) GetPath(key model.Key) (model.AbsPath, bool) {
	objectState, found := s.Get(key)
	if !found {
		return model.AbsPath{}, false
	}
	return objectState.GetAbsPath(), true
}

func (s *Registry) GetByPath(path string) (model.ObjectState, bool) {
	key, found := s.namingRegistry.KeyByPath(path)
	if !found {
		return nil, false
	}
	return s.Get(key)
}

func (s *Registry) Get(key model.Key) (model.ObjectState, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if v, ok := s.objects.Get(key.String()); ok {
		return v.(model.ObjectState), true
	}
	return nil, false
}

func (s *Registry) GetOrNil(key model.Key) model.ObjectState {
	v, _ := s.Get(key)
	return v
}

func (s *Registry) MustGet(key model.Key) model.ObjectState {
	state, found := s.Get(key)
	if !found {
		panic(errors.Errorf(`%s not found`, key.Desc()))
	}
	return state
}

func (s *Registry) CreateFrom(manifest model.ObjectManifest) (model.ObjectState, error) {
	objectState := manifest.NewObjectState()
	return objectState, s.Set(objectState)
}

// Set adds a NEW object to the registry.
// CRITICAL: This method ONLY works for NEW objects. If the object already exists, it returns an error.
//
// To update an existing object:
//
//	existingState, _ := registry.Get(key)
//	existingState.Local = newValue  // Just assign - no Set() call needed
//
// Common mistake: Calling Set() after updating an existing object's fields.
// See docs/CLI_OBJECT_LIFECYCLE.md for details.
func (s *Registry) Set(objectState model.ObjectState) error {
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

func (s *Registry) Remove(key model.Key) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.objects.Delete(key.String())
	s.namingRegistry.Detach(key)
}

func (s *Registry) GetOrCreateFrom(manifest model.ObjectManifest) (model.ObjectState, error) {
	if objectState, found := s.Get(manifest.Key()); found {
		objectState.SetManifest(manifest)
		return objectState, nil
	}

	return s.CreateFrom(manifest)
}
