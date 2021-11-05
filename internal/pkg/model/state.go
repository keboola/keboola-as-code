package model

import (
	"fmt"
	"strings"
	"sync"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type State struct {
	pathsState *PathsState
	sortBy     string
	lock       *sync.Mutex
	components *ComponentsMap
	objects    *orderedmap.OrderedMap
}

func NewState(logger *zap.SugaredLogger, fs filesystem.Fs, components *ComponentsMap, sortBy string) *State {
	ps, err := NewPathsState(fs)
	if err != nil {
		logger.Debug(utils.PrefixError(`error loading directory structure`, err).Error())
	}
	return &State{
		pathsState: ps,
		sortBy:     sortBy,
		lock:       &sync.Mutex{},
		components: components,
		objects:    utils.NewOrderedMap(),
	}
}

func (s *State) Components() *ComponentsMap {
	return s.components
}

func (s *State) PathsState() *PathsState {
	return s.pathsState.Clone()
}

func (s *State) ReloadPathsState() error {
	// Create a new paths state -> all paths are untracked
	ps, err := NewPathsState(s.pathsState.fs)
	if err != nil {
		return fmt.Errorf(`cannot reload paths state: %w`, err)
	}
	s.pathsState = ps

	// Track all known paths
	for _, object := range s.All() {
		if object.Manifest().State().IsPersisted() {
			s.TrackRecord(object.Manifest())
		}
	}
	return nil
}

func (s *State) TrackRecord(record Record) {
	for _, path := range record.GetRelatedPaths() {
		s.pathsState.MarkTracked(path)
	}
}

func (s *State) All() []ObjectState {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.objects.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value().(ObjectState).Manifest().SortKey(s.sortBy)
		bKey := b.Value().(ObjectState).Manifest().SortKey(s.sortBy)
		return aKey < bKey
	})

	var out []ObjectState
	for _, key := range s.objects.Keys() {
		// Get value
		v, _ := s.objects.Get(key)
		object := v.(ObjectState)

		// Skip deleted
		if object.Manifest().State().IsDeleted() {
			continue
		}

		out = append(out, object)
	}

	return out
}

func (s *State) StateObjects(stateType StateType) *StateObjects {
	switch stateType {
	case StateTypeRemote:
		return s.RemoteObjects()
	case StateTypeLocal:
		return s.LocalObjects()
	default:
		panic(fmt.Errorf(`unexpected StateType "%v"`, stateType))
	}
}

func (s *State) LocalObjects() *StateObjects {
	return NewStateObjects(s, StateTypeLocal)
}

func (s *State) RemoteObjects() *StateObjects {
	return NewStateObjects(s, StateTypeRemote)
}

func (s *State) Branches() (branches []*BranchState) {
	for _, object := range s.All() {
		if v, ok := object.(*BranchState); ok {
			branches = append(branches, v)
		}
	}
	return branches
}

// SearchForBranches by ID and name.
func (s *State) SearchForBranches(str string) []*BranchState {
	matches := make([]*BranchState, 0)
	for _, object := range s.Branches() {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// SearchForBranch by ID and name.
func (s *State) SearchForBranch(str string) (*BranchState, error) {
	branches := s.SearchForBranches(str)
	switch len(branches) {
	case 1:
		// ok, one match
		return branches[0], nil
	case 0:
		return nil, fmt.Errorf(`no branch matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple branches match the specified "%s"`, str)
	}
}

func (s *State) Configs() (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *State) ConfigsFrom(branch BranchKey) (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			if v.BranchId != branch.Id {
				continue
			}
			configs = append(configs, v)
		}
	}
	return configs
}

// SearchForConfigs by ID and name.
func (s *State) SearchForConfigs(str string, branch BranchKey) []*ConfigState {
	matches := make([]*ConfigState, 0)
	for _, object := range s.ConfigsFrom(branch) {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// SearchForConfig by ID and name.
func (s *State) SearchForConfig(str string, branch BranchKey) (*ConfigState, error) {
	configs := s.SearchForConfigs(str, branch)
	switch len(configs) {
	case 1:
		// ok, one match
		return configs[0], nil
	case 0:
		return nil, fmt.Errorf(`no config matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple configs match the specified "%s"`, str)
	}
}

func (s *State) ConfigRows() (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *State) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			if v.BranchId != config.BranchId || v.ComponentId != config.ComponentId || v.ConfigId != config.Id {
				continue
			}
			rows = append(rows, v)
		}
	}
	return rows
}

// SearchForConfigRows by ID and name.
func (s *State) SearchForConfigRows(str string, config ConfigKey) []*ConfigRowState {
	matches := make([]*ConfigRowState, 0)
	for _, object := range s.ConfigRowsFrom(config) {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// SearchForConfigRow by ID and name.
func (s *State) SearchForConfigRow(str string, config ConfigKey) (*ConfigRowState, error) {
	rows := s.SearchForConfigRows(str, config)
	switch len(rows) {
	case 1:
		// ok, one match
		return rows[0], nil
	case 0:
		return nil, fmt.Errorf(`no row matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple rows match the specified "%s"`, str)
	}
}

func (s *State) Get(key Key) (ObjectState, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if v, ok := s.objects.Get(key.String()); ok {
		return v.(ObjectState), true
	}
	return nil, false
}

func (s *State) MustGet(key Key) ObjectState {
	state, found := s.Get(key)
	if !found {
		panic(fmt.Errorf(`%s not found`, key.Desc()))
	}
	return state
}

func (s *State) CreateFrom(objectManifest Record) (ObjectState, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	key := objectManifest.Key()
	if _, found := s.objects.Get(key.String()); found {
		return nil, fmt.Errorf(`object "%s" already exists`, key.Desc())
	}

	objectState := objectManifest.NewObjectState()
	s.objects.Set(key.String(), objectState)
	return objectState, nil
}

func (s *State) GetOrCreateFrom(objectManifest Record) (ObjectState, error) {
	if objectState, found := s.Get(objectManifest.Key()); found {
		objectState.SetManifest(objectManifest)
		return objectState, nil
	}

	return s.CreateFrom(objectManifest)
}

func (s *State) IsTracked(path string) bool {
	return s.pathsState.IsTracked(path)
}

func (s *State) IsUntracked(path string) bool {
	return s.pathsState.IsUntracked(path)
}

// TrackedPaths returns all tracked paths.
func (s *State) TrackedPaths() []string {
	return s.pathsState.TrackedPaths()
}

// UntrackedPaths returns all untracked paths.
func (s *State) UntrackedPaths() []string {
	return s.pathsState.UntrackedPaths()
}

func (s *State) UntrackedDirs() (dirs []string) {
	return s.pathsState.UntrackedPaths()
}

func (s *State) UntrackedDirsFrom(base string) (dirs []string) {
	return s.pathsState.UntrackedDirsFrom(base)
}

func (s *State) IsFile(path string) bool {
	return s.pathsState.IsFile(path)
}

func (s *State) IsDir(path string) bool {
	return s.pathsState.IsDir(path)
}

func (s *State) LogUntrackedPaths(logger *zap.SugaredLogger) {
	s.pathsState.LogUntrackedPaths(logger)
}

// matchObjectIdOrName returns true if str == objectId or objectName contains str.
func matchObjectIdOrName(str string, object ObjectIdAndName) bool {
	if cast.ToString(object.ObjectId()) == str {
		return true
	}

	// Matched by name
	return strings.Contains(strings.ToLower(object.ObjectName()), strings.ToLower(str))
}

type StateType int

const (
	StateTypeLocal StateType = iota
	StateTypeRemote
)

type StateObjects struct {
	state     *State
	stateType StateType
}

func NewStateObjects(state *State, stateType StateType) *StateObjects {
	return &StateObjects{state: state, stateType: stateType}
}

func (f *StateObjects) All() []Object {
	var out []Object
	for _, object := range f.state.All() {
		if object.HasState(f.stateType) {
			out = append(out, object.GetState(f.stateType))
		}
	}
	return out
}

func (f *StateObjects) Get(key Key) (Object, bool) {
	objectState, found := f.state.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		return nil, false
	}
	return objectState.GetState(f.stateType), true
}

func (f *StateObjects) MustGet(key Key) Object {
	objectState, found := f.state.Get(key)
	if !found || !objectState.HasState(f.stateType) {
		panic(fmt.Errorf(`%s not found`, key.Desc()))
	}
	return objectState.GetState(f.stateType)
}

func (f *StateObjects) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow) {
	var out []*ConfigRow
	for _, row := range f.state.ConfigRowsFrom(config) {
		if row.HasState(f.stateType) {
			out = append(out, row.GetState(f.stateType).(*ConfigRow))
		}
	}
	return out
}
