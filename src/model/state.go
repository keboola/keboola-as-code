package model

import (
	"keboola-as-code/src/utils"
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
	Remote       *Branch
	Local        *Branch
	Manifest     *ManifestBranch
	MetadataFile string
}

type ComponentState struct {
	Remote *Component
}

type ConfigState struct {
	Remote       *Config
	Local        *Config
	Manifest     *ManifestConfig
	MetadataFile string
	ConfigFile   string
}

type ConfigRowState struct {
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

func (s *State) SetBranchRemoteState(branch *Branch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getBranchStateByKey(branch.UniqId())
	state.Remote = branch
}

func (s *State) SetBranchLocalState(branch *Branch, manifest *ManifestBranch, metadataFile string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	state := s.getBranchStateByKey(branch.UniqId())
	state.Local = branch
	state.Manifest = manifest
	state.MetadataFile = metadataFile
}

func (s *State) SetComponentRemoteState(component *Component) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getComponentStateByKey(component.UniqId())
	state.Remote = component
}

func (s *State) SetConfigRemoteState(config *Config) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigStateByKey(config.UniqId())
	state.Remote = config
}

func (s *State) SetConfigLocalState(config *Config, manifest *ManifestConfig, metadataFile, configFile string) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	s.MarkPathTracked(configFile)
	state := s.getConfigStateByKey(config.UniqId())
	state.Local = config
	state.Manifest = manifest
	state.MetadataFile = metadataFile
	state.ConfigFile = configFile
}

func (s *State) SetConfigRowRemoteState(configRow *ConfigRow) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigRowStateByKey(configRow.UniqId())
	state.Remote = configRow
}

func (s *State) SetConfigRowLocalState(configRow *ConfigRow, manifest *ManifestConfigRow, metadataFile, configFile string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(metadataFile)
	s.MarkPathTracked(configFile)
	state := s.getConfigRowStateByKey(configRow.UniqId())
	state.Local = configRow
	state.Manifest = manifest
}

func (s *State) getBranchStateByKey(key string) *BranchState {
	if _, ok := s.branches[key]; !ok {
		s.branches[key] = &BranchState{}
	}
	return s.branches[key]
}

func (s *State) getComponentStateByKey(key string) *ComponentState {
	if _, ok := s.components[key]; !ok {
		s.components[key] = &ComponentState{}
	}
	return s.components[key]
}

func (s *State) getConfigStateByKey(key string) *ConfigState {
	if _, ok := s.configs[key]; !ok {
		s.configs[key] = &ConfigState{}
	}
	return s.configs[key]
}

func (s *State) getConfigRowStateByKey(key string) *ConfigRowState {
	if _, ok := s.configRows[key]; !ok {
		s.configRows[key] = &ConfigRowState{}
	}
	return s.configRows[key]
}
