package remote

import "fmt"

type State struct {
	BranchesById       map[int]*Branch
	ConfigurationsById map[string]*Configuration
}

func (s *State) BranchById(id int) (*Branch, bool) {
	branch, found := s.BranchesById[id]
	return branch, found
}

func (s *State) BranchByName(name string) (*Branch, bool) {
	for _, branch := range s.BranchesById {
		if branch.Name == name {
			return branch, true
		}
	}
	return nil, false
}

func (s *State) ConfigurationById(branchId int, componentId string, configId int) (*Configuration, bool) {
	id := configurationId(branchId, componentId, configId)
	configuration, found := s.ConfigurationsById[id]
	return configuration, found
}

func (s *State) ConfigurationByName(name string) (*Configuration, bool) {
	for _, configuration := range s.ConfigurationsById {
		if configuration.Name == name {
			return configuration, true
		}
	}
	return nil, false
}

func configurationId(branchId int, componentId string, configId int) string {
	return fmt.Sprintf("%d_%s_%d", branchId, componentId, configId)
}
