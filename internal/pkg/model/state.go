package model

import (
	"fmt"
	"sync"

	"github.com/iancoleman/orderedmap"
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

func (s *State) Configs() (configs []*ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigState); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *State) ConfigRows() (rows []*ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*ConfigRowState); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *State) Get(key Key) ObjectState {
	object, err := s.GetOrCreate(key)
	if err != nil {
		panic(err)
	}

	if object == nil {
		panic(fmt.Errorf(`object "%s" not found`, key.String()))
	}
	return object
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
