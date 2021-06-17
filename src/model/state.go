package model

import (
	"fmt"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"sync"
)

type State struct {
	mutex          *sync.Mutex
	error          *utils.Error
	branchesById   map[int]*Branch
	componentsById map[string]*Component
	configsById    map[string]*Config
}

func NewState() *State {
	return &State{
		mutex:          &sync.Mutex{},
		error:          &utils.Error{},
		branchesById:   make(map[int]*Branch),
		componentsById: make(map[string]*Component),
		configsById:    make(map[string]*Config),
	}
}

func (s *State) Error() *utils.Error {
	return s.error
}

func (s *State) AddError(err error) {
	s.error.Add(err)
}

func (s *State) Branches() map[int]*Branch {
	return s.branchesById
}

func (s *State) Configs() map[string]*Config {
	return s.configsById
}

func (s *State) BranchById(id int) (*Branch, error) {
	branch, found := s.branchesById[id]
	if !found {
		return nil, fmt.Errorf("branch \"%d\" not found", branch.Id)
	}
	return branch, nil
}

func (s *State) ConfigurationById(branchId int, componentId string, id string) (*Config, error) {
	key := configKey(branchId, componentId, id)
	configuration, found := s.configsById[key]
	if !found {
		return nil, fmt.Errorf("config id: \"%s\", componentId: \"%s\", branch id: \"%d\" not found", id, componentId, branchId)
	}
	return configuration, nil
}

func (s *State) AddBranch(branch *Branch) bool {
	if err := validator.Validate(branch); err != nil {
		s.AddError(fmt.Errorf("branch is not valid:%s", err))
		return false
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.branchesById[branch.Id] = branch
	return true
}

func (s *State) AddComponent(component *Component) bool {
	if err := validator.Validate(component); err != nil {
		s.AddError(fmt.Errorf("component is not valid:%s", err))
		return false
	}

	func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		id := componentKey(component.BranchId, component.Id)
		s.componentsById[id] = component
	}()

	if ok := s.AddConfigs(component.Configs); !ok {
		return false
	}
	return true
}

func (s *State) AddConfigs(configs []*Config) bool {
	ok := true
	for _, config := range configs {
		if cOk := s.AddConfig(config); !cOk {
			ok = false
		}
	}
	return ok
}

func (s *State) AddConfig(config *Config) bool {
	if err := validator.Validate(config); err != nil {
		s.AddError(fmt.Errorf("config is not valid:%s", err))
		return false
	}

	if _, ok := s.branchesById[config.BranchId]; !ok {
		s.AddError(fmt.Errorf("branch \"%d\" not found", config.BranchId))
		return false
	}

	// The order of the rows does not matter, ... sort for easy testing
	config.SortRows()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := configKey(config.BranchId, config.ComponentId, config.Id)
	s.configsById[key] = config
	return true
}
