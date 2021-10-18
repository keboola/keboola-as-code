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

type pathsState = PathsState

type State struct {
	*pathsState
	sortBy     string
	mutex      *sync.Mutex
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
		mutex:      &sync.Mutex{},
		components: components,
		objects:    utils.NewOrderedMap(),
	}
}

func (s *State) Components() *ComponentsMap {
	return s.components
}

func (s *State) Relations() Relations {
	return make(Relations, 0)
}

func (s *State) All() []ObjectState {
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

func (s *State) Get(key Key) ObjectState {
	if v, ok := s.objects.Get(key.String()); ok {
		return v.(ObjectState)
	}
	panic(fmt.Errorf(`%s not found`, key.Desc()))
}

func (s *State) GetOrCreate(key Key) (ObjectState, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if v, ok := s.objects.Get(key.String()); ok {
		// Get
		return v.(ObjectState), nil
	} else {
		// Create
		var object ObjectState
		switch key.(type) {
		case BranchKey:
			object = &BranchState{}
		case ConfigKey:
			object = &ConfigState{}
		case ConfigRowKey:
			object = &ConfigRowState{}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, key))
		}

		s.objects.Set(key.String(), object)
		return object, nil
	}
}

// matchObjectIdOrName returns true if str == objectId or objectName contains str.
func matchObjectIdOrName(str string, object Object) bool {
	if cast.ToString(object.ObjectId()) == str {
		return true
	}

	// Matched by name
	return strings.Contains(strings.ToLower(object.ObjectName()), strings.ToLower(str))
}
