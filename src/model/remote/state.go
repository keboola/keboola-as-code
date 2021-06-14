package remote

import (
	"fmt"
	"sync"
)

type State struct {
	mutex                *sync.Mutex
	branchesById         map[int]*Branch
	componentsByBranchId map[int][]*Component
	configurationsById   map[string]*Config
}

func NewState() *State {
	return &State{
		mutex:                &sync.Mutex{},
		branchesById:         make(map[int]*Branch),
		componentsByBranchId: make(map[int][]*Component),
		configurationsById:   make(map[string]*Config),
	}
}

func (s *State) Branches() map[int]*Branch {
	return s.branchesById
}

func (s *State) Configurations() map[string]*Config {
	return s.configurationsById
}

func (s *State) BranchById(id int) (*Branch, bool) {
	branch, found := s.branchesById[id]
	return branch, found
}

func (s *State) BranchByName(name string) (*Branch, bool) {
	for _, branch := range s.branchesById {
		if branch.Name == name {
			return branch, true
		}
	}
	return nil, false
}

func (s *State) ConfigurationById(branchId int, componentId string, configId string) (*Config, bool) {
	id := configurationId(branchId, componentId, configId)
	configuration, found := s.configurationsById[id]
	return configuration, found
}

func (s *State) ConfigurationByName(name string) (*Config, bool) {
	for _, configuration := range s.configurationsById {
		if configuration.Name == name {
			return configuration, true
		}
	}
	return nil, false
}

func (s *State) AddBranch(branch *Branch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.branchesById[branch.Id] = branch
}

func (s *State) AddComponent(component *Component) {
	// We must unlock mutex before AddConfiguration call
	func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		bId := component.BranchId
		s.componentsByBranchId[bId] = append(s.componentsByBranchId[bId], component)
	}()

	for _, configuration := range component.Configs {
		s.AddConfiguration(configuration)
	}
	component.Configs = nil // no more required
}

func (s *State) AddConfiguration(configuration *Config) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id := configurationId(configuration.BranchId, configuration.ComponentId, configuration.Id)
	s.configurationsById[id] = configuration
}

func configurationId(branchId int, componentId string, configId string) string {
	return fmt.Sprintf("%d_%s_%s", branchId, componentId, configId)
}
