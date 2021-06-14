package model

import (
	"fmt"
	"keboola-as-code/src/validator"
	"sync"
)

type State struct {
	mutex          *sync.Mutex
	branchesById   map[int]*Branch
	componentsById map[string]*Component
	configsById    map[string]*Config
}

func NewState() *State {
	return &State{
		mutex:          &sync.Mutex{},
		branchesById:   make(map[int]*Branch),
		componentsById: make(map[string]*Component),
		configsById:    make(map[string]*Config),
	}
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

func (s *State) AddBranch(branch *Branch) error {
	if err := validator.Validate(branch); err != nil {
		panic(fmt.Errorf("branch is not valid\n:%s", err))
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.branchesById[branch.Id] = branch
	return nil
}

func (s *State) AddComponent(component *Component) error {
	if err := validator.Validate(component); err != nil {
		panic(fmt.Errorf("component is not valid\n:%s", err))
	}

	func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		id := componentKey(component.BranchId, component.Id)
		s.componentsById[id] = component
	}()

	if err := s.AddConfigs(component.Configs); err != nil {
		return err
	}
	return nil
}

func (s *State) AddConfigs(configs []*Config) error {
	for _, config := range configs {
		if err := s.AddConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) AddConfig(config *Config) error {
	if err := validator.Validate(config); err != nil {
		return fmt.Errorf("config is not valid\n:%s", err)
	}

	if _, ok := s.branchesById[config.BranchId]; !ok {
		return fmt.Errorf("branch \"%d\" not found", config.BranchId)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := configKey(config.BranchId, config.ComponentId, config.Id)
	s.configsById[key] = config
	return nil
}
