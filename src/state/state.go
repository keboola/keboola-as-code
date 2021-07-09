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

// State - Local and Remote state of the project
type State struct {
	mutex        *sync.Mutex
	api          *remote.StorageApi
	remoteErrors *utils.Error
	localErrors  *utils.Error
	paths        *PathsState
	manifest     *manifest.Manifest
	components   map[string]*model.Component
	branches     map[string]*BranchState
	configs      map[string]*ConfigState
	configRows   map[string]*ConfigRowState
}

type ObjectState interface {
	Kind() model.Kind
	LocalState() model.ValueWithKey
	RemoteState() model.ValueWithKey
	Manifest() manifest.Record
	UpdateManifest(m *manifest.Manifest)
	RelativePath() string
}

type BranchState struct {
	*manifest.BranchManifest
	Remote *model.Branch `validate:"dive"`
	Local  *model.Branch `validate:"dive"`
}

type ConfigState struct {
	*manifest.ConfigManifest
	Component *model.Component `validate:"dive"`
	Remote    *model.Config    `validate:"dive"`
	Local     *model.Config    `validate:"dive"`
}

type ConfigRowState struct {
	*manifest.ConfigRowManifest
	Remote *model.ConfigRow `validate:"dive"`
	Local  *model.ConfigRow `validate:"dive"`
}

type keyValuePair struct {
	key   string
	state ObjectState
}

type stateValidator struct {
	error *utils.Error
}

func (s *stateValidator) validate(kind string, v interface{}) {
	if err := validator.Validate(v); err != nil {
		s.error.AddSubError(fmt.Sprintf("%s is not valid", kind), err)
	}
}

// LoadState - remote and local
func LoadState(m *manifest.Manifest, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi, remote bool) (*State, bool) {
	state := NewState(m.ProjectDir, api, m)

	// Token and manifest project ID must be same
	if m.Project.Id != api.ProjectId() {
		state.AddLocalError(fmt.Errorf("used token is from the project \"%d\", but it must be from the project \"%d\"", api.ProjectId(), m.Project.Id))
		return state, false
	}

	if remote {
		logger.Debugf("Loading project remote state.")
		state.LoadRemoteState(ctx)
	}

	logger.Debugf("Loading local state.")
	state.LoadLocalState()

	ok := state.LocalErrors().Len() == 0 && state.RemoteErrors().Len() == 0
	return state, ok
}

func NewState(projectDir string, api *remote.StorageApi, m *manifest.Manifest) *State {
	s := &State{
		mutex:        &sync.Mutex{},
		api:          api,
		remoteErrors: &utils.Error{},
		localErrors:  &utils.Error{},
		manifest:     m,
		branches:     make(map[string]*BranchState),
		components:   make(map[string]*model.Component),
		configs:      make(map[string]*ConfigState),
		configRows:   make(map[string]*ConfigRowState),
	}
	s.paths = NewPathsState(projectDir, s.localErrors)
	return s
}

func (s *State) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *State) Validate() *utils.Error {
	v := &stateValidator{}
	for _, component := range s.Components() {
		v.validate("component", component)
	}
	for _, objectState := range s.All() {
		v.validate(objectState.Kind().Name, objectState)
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
	sort.SliceStable(branches, func(i, j int) bool {
		return branches[i].SortKey(s.manifest.SortBy) < branches[j].SortKey(s.manifest.SortBy)
	})
	return branches
}

func (s *State) Components() []*model.Component {
	var components []*model.Component
	for _, c := range s.components {
		components = append(components, c)
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].Id < components[j].Id
	})
	return components
}

func (s *State) Configs() []*ConfigState {
	var configs []*ConfigState
	for _, c := range s.configs {
		configs = append(configs, c)
	}
	sort.SliceStable(configs, func(i, j int) bool {
		return configs[i].SortKey(s.manifest.SortBy) < configs[j].SortKey(s.manifest.SortBy)
	})
	return configs
}

func (s *State) ConfigRows() []*ConfigRowState {
	var configRows []*ConfigRowState
	for _, r := range s.configRows {
		configRows = append(configRows, r)
	}
	sort.SliceStable(configRows, func(i, j int) bool {
		return configRows[i].SortKey(s.manifest.SortBy) < configRows[j].SortKey(s.manifest.SortBy)
	})
	return configRows
}

func (s *State) GetBranch(key model.BranchKey, create bool) *BranchState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.branches[keyStr]; !ok {
		if !create {
			return nil
		}
		s.branches[keyStr] = &BranchState{}
	}
	return s.branches[keyStr]
}

func (s *State) GetComponent(key model.ComponentKey) *model.Component {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if component, found := s.components[key.String()]; found {
		return component
	}
	return nil
}

func (s *State) GetConfig(key model.ConfigKey, create bool) *ConfigState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.configs[keyStr]; !ok {
		if !create {
			return nil
		}
		s.configs[keyStr] = &ConfigState{}
	}
	return s.configs[keyStr]
}

func (s *State) GetConfigRow(key model.ConfigRowKey, create bool) *ConfigRowState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.configRows[keyStr]; !ok {
		if !create {
			return nil
		}
		s.configRows[keyStr] = &ConfigRowState{}
	}
	return s.configRows[keyStr]
}

func (s *State) SetBranchRemoteState(remote *model.Branch) {
	state := s.GetBranch(remote.BranchKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.BranchManifest == nil {
		state.BranchManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*manifest.BranchManifest)
		state.UpdateManifest(s.manifest)
	}
}

func (s *State) SetBranchLocalState(local *model.Branch, m *manifest.BranchManifest) {
	branch := s.GetBranch(local.BranchKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	branch.Local = local
	branch.BranchManifest = m
	s.MarkPathTracked(m.MetaFilePath())
}

func (s *State) setComponent(component *model.Component) {
	if component == nil {
		panic(fmt.Errorf("component is not set"))
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.components[component.String()] = component
}

func (s *State) SetConfigRemoteState(component *model.Component, remote *model.Config) {
	s.setComponent(component)
	state := s.GetConfig(remote.ConfigKey, true)
	state.Component = component
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.ConfigManifest == nil {
		state.ConfigManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*manifest.ConfigManifest)
		state.UpdateManifest(s.manifest)
	}
}

func (s *State) SetConfigLocalState(component *model.Component, local *model.Config, m *manifest.ConfigManifest) {
	s.setComponent(component)
	state := s.GetConfig(local.ConfigKey, true)
	state.Component = component
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Local = local
	state.ConfigManifest = m
	s.MarkPathTracked(m.MetaFilePath())
	s.MarkPathTracked(m.ConfigFilePath())
}

func (s *State) SetConfigRowRemoteState(remote *model.ConfigRow) {
	state := s.GetConfigRow(remote.ConfigRowKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.ConfigRowManifest == nil {
		state.ConfigRowManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*manifest.ConfigRowManifest)
		state.UpdateManifest(s.manifest)
	}
}

func (s *State) SetConfigRowLocalState(local *model.ConfigRow, m *manifest.ConfigRowManifest) {
	state := s.GetConfigRow(local.ConfigRowKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Local = local
	state.ConfigRowManifest = m
	s.MarkPathTracked(m.MetaFilePath())
	s.MarkPathTracked(m.ConfigFilePath())
}

func (b *BranchState) LocalState() model.ValueWithKey {
	return b.Local
}

func (c *ConfigState) LocalState() model.ValueWithKey {
	return c.Local
}

func (r *ConfigRowState) LocalState() model.ValueWithKey {
	return r.Local
}

func (b *BranchState) RemoteState() model.ValueWithKey {
	return b.Remote
}

func (c *ConfigState) RemoteState() model.ValueWithKey {
	return c.Remote
}

func (r *ConfigRowState) RemoteState() model.ValueWithKey {
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
