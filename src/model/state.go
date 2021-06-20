package model

import (
	"fmt"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"sort"
	"sync"
)

type State struct {
	mutex        *sync.Mutex
	remoteErrors *utils.Error
	localErrors  *utils.Error
	paths        *PathsState
	branches     map[string]*BranchState
	components   map[string]*ComponentState
	configs      map[string]*ConfigState
	configRows   map[string]*ConfigRowState
}

type ObjectState interface {
	Kind() string
	LocalState() interface{}
	RemoteState() interface{}
	Manifest() interface{}
	RelativePath() string
}

type BranchState struct {
	*BranchManifest
	Id     int
	Remote *Branch
	Local  *Branch
}

type ComponentState struct {
	BranchId int
	Id       string
	Remote   *Component
}

type ConfigState struct {
	*ConfigManifest
	BranchId    int
	ComponentId string
	Id          string
	Remote      *Config
	Local       *Config
}

type ConfigRowState struct {
	*ConfigRowManifest
	BranchId    int
	ComponentId string
	ConfigId    string
	Id          string
	Remote      *ConfigRow
	Local       *ConfigRow
}

type stateValidator struct {
	error *utils.Error
}

func (s *stateValidator) AddError(err error) {
	s.error.Add(err)
}

func (s *stateValidator) validate(kind string, v interface{}) {
	if err := validator.Validate(v); err != nil {
		s.AddError(fmt.Errorf("%s is not valid: %s", kind, err))
	}
}

func NewState(projectDir string) *State {
	s := &State{
		mutex:        &sync.Mutex{},
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

func (s *State) Validate() *utils.Error {
	v := &stateValidator{}
	for _, c := range s.Components() {
		v.validate("component", c.Remote)
	}
	for _, objectState := range s.All() {
		v.validate(objectState.Kind(), objectState.RemoteState())
		v.validate(objectState.Kind(), objectState.RelativePath())
		v.validate(objectState.Kind()+" manifest record", objectState.Manifest())
	}
	return v.error
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

func (s *State) All() []ObjectState {
	var all []ObjectState
	for _, branch := range s.Branches() {
		all = append(all, branch)
	}
	for _, config := range s.Configs() {
		all = append(all, config)
	}
	for _, row := range s.ConfigRows() {
		all = append(all, row)
	}
	return all
}

func (s *State) Branches() []*BranchState {
	var branches []*BranchState
	for _, b := range s.branches {
		branches = append(branches, b)
	}
	sort.SliceStable(branches, func(i, j int) bool {
		return branches[i].CmpValue() < branches[j].CmpValue()
	})
	return branches
}

func (s *State) Components() []*ComponentState {
	var components []*ComponentState
	for _, c := range s.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].CmpValue() < components[j].CmpValue()
	})
	return components
}

func (s *State) Configs() []*ConfigState {
	var configs []*ConfigState
	for _, c := range s.configs {
		configs = append(configs, c)
	}
	sort.SliceStable(configs, func(i, j int) bool {
		return configs[i].CmpValue() < configs[j].CmpValue()
	})
	return configs
}

func (s *State) ConfigRows() []*ConfigRowState {
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
	state := s.getBranchState(branch.Id)
	state.Remote = branch
	if state.BranchManifest == nil {
		state.BranchManifest = branch.GenerateManifest()
	}
}

func (s *State) SetBranchLocalState(branch *Branch, manifest *BranchManifest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetadataFilePath())
	state := s.getBranchState(branch.Id)
	state.Local = branch
	state.BranchManifest = manifest
}

func (s *State) SetComponentRemoteState(component *Component) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getComponentState(component.BranchId, component.Id)
	state.Remote = component
}

func (s *State) SetConfigRemoteState(config *Config) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigState(config.BranchId, config.ComponentId, config.Id)
	state.Remote = config
	if state.ConfigManifest == nil {
		branch := s.getBranchState(config.BranchId)
		state.ConfigManifest = config.GenerateManifest(branch.BranchManifest)
	}
}

func (s *State) SetConfigLocalState(config *Config, manifest *ConfigManifest) {
	config.SortRows()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetadataFilePath())
	s.MarkPathTracked(manifest.ConfigFilePath())
	state := s.getConfigState(config.BranchId, config.ComponentId, config.Id)
	state.Local = config
	state.ConfigManifest = manifest
}

func (s *State) SetConfigRowRemoteState(row *ConfigRow) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigRowState(row.BranchId, row.ComponentId, row.ConfigId, row.Id)
	state.Remote = row
	if state.ConfigRowManifest == nil {
		config := s.getConfigState(row.BranchId, row.ComponentId, row.ConfigId)
		state.ConfigRowManifest = row.GenerateManifest(config.ConfigManifest)
		config.ConfigManifest.Rows = append(config.ConfigManifest.Rows, state.ConfigRowManifest)
	}
}

func (s *State) SetConfigRowLocalState(row *ConfigRow, manifest *ConfigRowManifest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetadataFilePath())
	s.MarkPathTracked(manifest.ConfigFilePath())
	state := s.getConfigRowState(row.BranchId, row.ComponentId, row.ConfigId, row.Id)
	state.Local = row
	state.ConfigRowManifest = manifest
}

func (s *State) getBranchState(branchId int) *BranchState {
	key := fmt.Sprintf("%d", branchId)
	if _, ok := s.branches[key]; !ok {
		s.branches[key] = &BranchState{
			Id: branchId,
		}
	}
	return s.branches[key]
}

func (s *State) getComponentState(branchId int, componentId string) *ComponentState {
	key := fmt.Sprintf("%d_%s", branchId, componentId)
	if _, ok := s.components[key]; !ok {
		s.components[key] = &ComponentState{
			BranchId: branchId,
			Id:       componentId,
		}
	}
	return s.components[key]
}

func (s *State) getConfigState(branchId int, componentId, configId string) *ConfigState {
	key := fmt.Sprintf("%d_%s_%s", branchId, componentId, configId)
	if _, ok := s.configs[key]; !ok {
		s.configs[key] = &ConfigState{
			BranchId:    branchId,
			ComponentId: componentId,
			Id:          configId,
		}
	}
	return s.configs[key]
}

func (s *State) getConfigRowState(branchId int, componentId, configId, rowId string) *ConfigRowState {
	key := fmt.Sprintf("%d_%s__%s_%s", branchId, componentId, configId, rowId)
	if _, ok := s.configRows[key]; !ok {
		s.configRows[key] = &ConfigRowState{
			BranchId:    branchId,
			ComponentId: componentId,
			ConfigId:    configId,
			Id:          rowId,
		}
	}
	return s.configRows[key]
}

func (b *BranchState) Kind() string {
	return "branch"
}

func (c *ConfigState) Kind() string {
	return "config"
}

func (r *ConfigRowState) Kind() string {
	return "configRow"
}

func (b *BranchState) LocalState() interface{} {
	return b.Local
}

func (c *ConfigState) LocalState() interface{} {
	return c.Local
}

func (r *ConfigRowState) LocalState() interface{} {
	return r.Local
}

func (b *BranchState) RemoteState() interface{} {
	return b.Remote
}

func (c *ConfigState) RemoteState() interface{} {
	return c.Remote
}

func (r *ConfigRowState) RemoteState() interface{} {
	return r.Remote
}

func (b *BranchState) Manifest() interface{} {
	return b.BranchManifest
}

func (c *ConfigState) Manifest() interface{} {
	return c.ConfigManifest
}

func (r *ConfigRowState) Manifest() interface{} {
	return r.ConfigRowManifest
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
