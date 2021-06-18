package model

import (
	"fmt"
	"keboola-as-code/src/utils"
	"sort"
	"sync"
)

type State struct {
	mutex        *sync.Mutex
	projectDir   string
	metadataDir  string
	remoteErrors *utils.Error
	localErrors  *utils.Error
	paths        *PathsState
	branches     map[string]*BranchState
	components   map[string]*ComponentState
	configs      map[string]*ConfigState
	configRows   map[string]*ConfigRowState
}

type BranchState struct {
	Id           int
	Remote       *Branch
	Local        *Branch
	Manifest     *ManifestBranch
	MetadataFile string
}

type ComponentState struct {
	BranchId int
	Id       string
	Remote   *Component
}

type ConfigState struct {
	BranchId     int
	ComponentId  string
	Id           string
	Remote       *Config
	Local        *Config
	Manifest     *ManifestConfig
	MetadataFile string
	ConfigFile   string
}

type ConfigRowState struct {
	BranchId     int
	ComponentId  string
	ConfigId     string
	Id           string
	Remote       *ConfigRow
	Local        *ConfigRow
	Manifest     *ManifestConfigRow
	MetadataFile string
	ConfigFile   string
}

func NewState(projectDir, metadataDir string) *State {
	s := &State{
		mutex:        &sync.Mutex{},
		projectDir:   projectDir,
		metadataDir:  metadataDir,
		remoteErrors: &utils.Error{},
		localErrors:  &utils.Error{},
		branches:     make(map[string]*BranchState),
		components:   make(map[string]*ComponentState),
		configs:      make(map[string]*ConfigState),
		configRows:   make(map[string]*ConfigRowState),
	}
	s.paths = NewPathsState(projectDir, s.localErrors)
	return s
}

func (s *State) ProjectDir() string {
	return s.projectDir
}

func (s *State) MetadataDir() string {
	return s.metadataDir
}

func (s *State) Validate() *utils.Error {
	return validateState(s)
}

func (s *State) MarkPathTracked(path string) {
	s.paths.MarkTracked(path)
}

func (s *State) TrackedPaths() []string {
	return s.paths.Tracked()
}
func (s *State) UntrackedPaths() []string {
	return s.paths.Untracked()
}

func (s *State) RemoteErrors() *utils.Error {
	return s.remoteErrors
}

func (s *State) LocalErrors() *utils.Error {
	return s.localErrors
}

func (s *State) AddRemoteError(err error) {
	s.remoteErrors.Add(err)
}

func (s *State) AddLocalError(err error) {
	s.localErrors.Add(err)
}

func (s *State) Branches() map[string]*BranchState {
	return s.branches
}

func (s *State) Components() map[string]*ComponentState {
	return s.components
}

func (s *State) Configs() map[string]*ConfigState {
	return s.configs
}

func (s *State) ConfigRows() map[string]*ConfigRowState {
	return s.configRows
}

func (s *State) BranchesSlice() []*BranchState {
	var branches []*BranchState
	for _, b := range s.branches {
		branches = append(branches, b)
	}
	sort.SliceStable(branches, func(i, j int) bool {
		return branches[i].CmpValue() < branches[j].CmpValue()
	})
	return branches
}

func (s *State) ComponentsSlice() []*ComponentState {
	var components []*ComponentState
	for _, c := range s.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].CmpValue() < components[j].CmpValue()
	})
	return components
}

func (s *State) ConfigsSlice() []*ConfigState {
	var configs []*ConfigState
	for _, c := range s.configs {
		configs = append(configs, c)
	}
	sort.SliceStable(configs, func(i, j int) bool {
		return configs[i].CmpValue() < configs[j].CmpValue()
	})
	return configs
}

func (s *State) ConfigRowsSlice() []*ConfigRowState {
	var configRows []*ConfigRowState
	for _, r := range s.configRows {
		configRows = append(configRows, r)
	}
	sort.SliceStable(configRows, func(i, j int) bool {
		return configRows[i].CmpValue() < configRows[j].CmpValue()
	})
	return configRows
}

func (s *State) SetBranchRemoteState(branch *Branch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getBranchState(branch)
	state.Remote = branch
}

func (s *State) SetBranchLocalState(branch *Branch, manifest *ManifestBranch, metadataFile string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	state := s.getBranchState(branch)
	state.Local = branch
	state.Manifest = manifest
	state.MetadataFile = metadataFile
}

func (s *State) SetComponentRemoteState(component *Component) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getComponentState(component)
	state.Remote = component
}

func (s *State) SetConfigRemoteState(config *Config) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigState(config)
	state.Remote = config
}

func (s *State) SetConfigLocalState(config *Config, manifest *ManifestConfig, metadataFile, configFile string) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	s.MarkPathTracked(configFile)
	state := s.getConfigState(config)
	state.Local = config
	state.Manifest = manifest
	state.MetadataFile = metadataFile
	state.ConfigFile = configFile
}

func (s *State) SetConfigRowRemoteState(configRow *ConfigRow) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigRowState(configRow)
	state.Remote = configRow
}

func (s *State) SetConfigRowLocalState(configRow *ConfigRow, manifest *ManifestConfigRow, metadataFile, configFile string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	s.MarkPathTracked(configFile)
	state := s.getConfigRowState(configRow)
	state.Local = configRow
	state.Manifest = manifest
}

func (s *State) getBranchState(branch *Branch) *BranchState {
	key := branch.UniqId()
	if _, ok := s.branches[key]; !ok {
		s.branches[key] = &BranchState{
			Id: branch.Id,
		}
	}
	return s.branches[key]
}

func (s *State) getComponentState(component *Component) *ComponentState {
	key := component.UniqId()
	if _, ok := s.components[key]; !ok {
		s.components[key] = &ComponentState{
			BranchId: component.BranchId,
			Id:       component.Id,
		}
	}
	return s.components[key]
}

func (s *State) getConfigState(config *Config) *ConfigState {
	key := config.UniqId()
	if _, ok := s.configs[key]; !ok {
		s.configs[key] = &ConfigState{
			BranchId:    config.BranchId,
			ComponentId: config.ComponentId,
			Id:          config.Id,
		}
	}
	return s.configs[key]
}

func (s *State) getConfigRowState(configRow *ConfigRow) *ConfigRowState {
	key := configRow.UniqId()
	if _, ok := s.configRows[key]; !ok {
		s.configRows[key] = &ConfigRowState{
			BranchId:    configRow.BranchId,
			ComponentId: configRow.ComponentId,
			ConfigId:    configRow.ConfigId,
			Id:          configRow.Id,
		}
	}
	return s.configRows[key]
}

func (b *BranchState) CmpValue() string {
	return fmt.Sprintf("%d", b.Id)
}

func (c *ComponentState) CmpValue() string {
	return fmt.Sprintf("%d_%s", c.BranchId, c.Id)
}

func (c *ConfigState) CmpValue() string {
	name := ""
	if c.Remote != nil {
		name = c.Remote.Name
	} else if c.Local != nil {
		name = c.Local.Name
	}
	return fmt.Sprintf("%d_%s_%s", c.BranchId, c.ComponentId, name)
}

func (r *ConfigRowState) CmpValue() string {
	name := ""
	if r.Remote != nil {
		name = r.Remote.Name
	} else if r.Local != nil {
		name = r.Local.Name
	}
	return fmt.Sprintf("%d_%s_%s", r.BranchId, r.ComponentId, name)
}
