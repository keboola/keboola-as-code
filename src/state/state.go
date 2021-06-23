package state

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
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
	naming       *manifest.LocalNaming
	branches     map[string]*BranchState
	components   map[string]*ComponentState
	configs      map[string]*ConfigState
	configRows   map[string]*ConfigRowState
}

type ObjectState interface {
	Kind() string
	LocalState() interface{}
	RemoteState() interface{}
	Manifest() manifest.Record
	RelativePath() string
}

type BranchState struct {
	*manifest.BranchManifest
	Remote *model.Branch
	Local  *model.Branch
}

type ComponentState struct {
	*model.Component
}

type ConfigState struct {
	*manifest.ConfigManifest
	Remote *model.Config
	Local  *model.Config
}

type ConfigRowState struct {
	*manifest.ConfigRowManifest
	Remote *model.ConfigRow
	Local  *model.ConfigRow
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

type keyValuePair struct {
	key   string
	state ObjectState
}

func LoadState(manifest *manifest.Manifest, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) (*State, bool) {
	state := NewState(manifest.ProjectDir, manifest.Naming)

	logger.Debugf("Loading project remote state.")
	LoadRemoteState(state, ctx, api)

	logger.Debugf("Loading local state.")
	LoadLocalState(state, manifest, api)

	ok := state.LocalErrors().Len() == 0 && state.RemoteErrors().Len() == 0
	return state, ok
}

func NewState(projectDir string, naming *manifest.LocalNaming) *State {
	s := &State{
		mutex:        &sync.Mutex{},
		remoteErrors: &utils.Error{},
		localErrors:  &utils.Error{},
		naming:       naming,
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
		v.validate("component", c.Component)
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

func (s *State) LogUntrackedPaths(logger *zap.SugaredLogger) {
	untracked := s.UntrackedPaths()
	if len(untracked) > 0 {
		logger.Warn("Unknown paths found:")
		for _, path := range untracked {
			logger.Warn("  " + path)
		}
	}
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
	var all []keyValuePair
	for key, branch := range s.branches {
		all = append(all, keyValuePair{key, branch})
	}
	for key, config := range s.configs {
		all = append(all, keyValuePair{key, config})
	}
	for key, row := range s.configRows {
		all = append(all, keyValuePair{key, row})
	}

	// Sort by key: branch -> config -> config_row
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].key < all[j].key
	})

	// Convert to slice
	var slice []ObjectState
	for _, pair := range all {
		slice = append(slice, pair.state)
	}
	return slice
}

func (s *State) Branches() []*BranchState {
	var branches []*BranchState
	for _, b := range s.branches {
		branches = append(branches, b)
	}
	return branches
}

func (s *State) Components() []*ComponentState {
	var components []*ComponentState
	for _, c := range s.components {
		components = append(components, c)
	}
	return components
}

func (s *State) Configs() []*ConfigState {
	var configs []*ConfigState
	for _, c := range s.configs {
		configs = append(configs, c)
	}
	return configs
}

func (s *State) ConfigRows() []*ConfigRowState {
	var configRows []*ConfigRowState
	for _, r := range s.configRows {
		configRows = append(configRows, r)
	}
	return configRows
}

func (s *State) GetComponent(componentId string) (*model.Component, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s, ok := s.components[componentId]; ok {
		return s.Component, true
	}
	return nil, false
}

func (s *State) SetBranchRemoteState(branch *model.Branch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getBranchState(branch.BranchKey)
	state.Remote = branch
	if state.BranchManifest == nil {
		state.BranchManifest = manifest.NewBranchManifest(s.naming, branch)
	}
}

func (s *State) SetBranchLocalState(branch *model.Branch, manifest *manifest.BranchManifest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetaFilePath())
	state := s.getBranchState(branch.BranchKey)
	state.Local = branch
	state.BranchManifest = manifest
}

func (s *State) setComponentRemoteState(component *model.Component) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getComponentState(component.ComponentKey)
	state.Component = component
}

func (s *State) SetConfigRemoteState(component *model.Component, config *model.Config) {
	s.setComponentRemoteState(component)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigState(config.ConfigKey)
	state.Remote = config
	if state.ConfigManifest == nil {
		branch := s.getBranchState(config.BranchKey())
		state.ConfigManifest = manifest.NewConfigManifest(s.naming, branch.BranchManifest, component, config)
	}
}

func (s *State) SetConfigLocalState(component *model.Component, config *model.Config, manifest *manifest.ConfigManifest) {
	s.setComponentRemoteState(component)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetaFilePath())
	s.MarkPathTracked(manifest.ConfigFilePath())
	state := s.getConfigState(config.ConfigKey)
	state.Local = config
	state.ConfigManifest = manifest
}

func (s *State) SetConfigRowRemoteState(row *model.ConfigRow) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state := s.getConfigRowState(row.ConfigRowKey)
	state.Remote = row
	if state.ConfigRowManifest == nil {
		config := s.getConfigState(row.ConfigKey())
		state.ConfigRowManifest = manifest.NewConfigRowManifest(s.naming, config.ConfigManifest, row)
	}
}

func (s *State) SetConfigRowLocalState(row *model.ConfigRow, manifest *manifest.ConfigRowManifest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MarkPathTracked(manifest.MetaFilePath())
	s.MarkPathTracked(manifest.ConfigFilePath())
	state := s.getConfigRowState(row.ConfigRowKey)
	state.Local = row
	state.ConfigRowManifest = manifest
}

func (s *State) getBranchState(key model.BranchKey) *BranchState {
	keyStr := key.String()
	if _, ok := s.branches[keyStr]; !ok {
		s.branches[keyStr] = &BranchState{}
	}
	return s.branches[keyStr]
}

func (s *State) getComponentState(key model.ComponentKey) *ComponentState {
	keyStr := key.String()
	if _, ok := s.components[keyStr]; !ok {
		s.components[keyStr] = &ComponentState{}
	}
	return s.components[keyStr]
}

func (s *State) getConfigState(key model.ConfigKey) *ConfigState {
	keyStr := key.String()
	if _, ok := s.configs[keyStr]; !ok {
		s.configs[keyStr] = &ConfigState{}
	}
	return s.configs[keyStr]
}

func (s *State) getConfigRowState(key model.ConfigRowKey) *ConfigRowState {
	keyStr := key.String()
	if _, ok := s.configRows[keyStr]; !ok {
		s.configRows[keyStr] = &ConfigRowState{}
	}
	return s.configRows[keyStr]
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

func (b *BranchState) Manifest() manifest.Record {
	return b.BranchManifest
}

func (c *ConfigState) Manifest() manifest.Record {
	return c.ConfigManifest
}

func (r *ConfigRowState) Manifest() manifest.Record {
	return r.ConfigRowManifest
}

func (b *BranchState) GetName() string {
	if b.Remote != nil {
		return b.Remote.Name
	}
	if b.Local != nil {
		return b.Local.Name
	}
	return ""
}

func (c *ConfigState) GetName() string {
	if c.Remote != nil {
		return c.Remote.Name
	}
	if c.Local != nil {
		return c.Local.Name
	}
	return ""
}

func (r *ConfigRowState) GetName() string {
	if r.Remote != nil {
		return r.Remote.Name
	}
	if r.Local != nil {
		return r.Local.Name
	}
	return ""
}
