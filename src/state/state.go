package state

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"path/filepath"
	"sort"
	"sync"
)

// State - Local and Remote state of the project
type State struct {
	*Options
	mutex        *sync.Mutex
	remoteErrors *utils.Error
	localErrors  *utils.Error
	paths        *PathsState
	localManager *local.Manager
	newPersisted []model.ObjectState
	branches     map[string]*model.BranchState
	configs      map[string]*model.ConfigState
	configRows   map[string]*model.ConfigRowState
}

type Options struct {
	manifest        *manifest.Manifest
	api             *remote.StorageApi
	context         context.Context
	logger          *zap.SugaredLogger
	LoadLocalState  bool
	LoadRemoteState bool
	SkipNotFoundErr bool
}

type keyValuePair struct {
	key   string
	state model.ObjectState
}

func NewOptions(m *manifest.Manifest, api *remote.StorageApi, ctx context.Context, logger *zap.SugaredLogger) *Options {
	return &Options{
		manifest: m,
		api:      api,
		context:  ctx,
		logger:   logger,
	}
}

// LoadState - remote and local
func LoadState(options *Options) (state *State, ok bool) {
	state = newState(options)

	// Token and manifest project ID must be same
	if state.manifest.Project.Id != state.api.ProjectId() {
		state.AddLocalError(fmt.Errorf("used token is from the project \"%d\", but it must be from the project \"%d\"", state.api.ProjectId(), state.manifest.Project.Id))
		return state, false
	}

	if state.LoadRemoteState {
		state.logger.Debugf("Loading project remote state.")
		state.doLoadRemoteState()
	}

	if state.LoadLocalState {
		state.logger.Debugf("Loading local state.")
		state.doLoadLocalState()
	}

	state.validate()

	ok = state.LocalErrors().Len() == 0 && state.RemoteErrors().Len() == 0
	return state, ok
}

func newState(options *Options) *State {
	s := &State{
		Options:      options,
		mutex:        &sync.Mutex{},
		remoteErrors: utils.NewMultiError(),
		localErrors:  utils.NewMultiError(),
		branches:     make(map[string]*model.BranchState),
		configs:      make(map[string]*model.ConfigState),
		configRows:   make(map[string]*model.ConfigRowState),
	}
	s.localManager = local.NewManager(options.logger, options.manifest, s.api)
	s.paths = NewPathsState(s.manifest.ProjectDir, s.localErrors)
	return s
}

func (s *State) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *State) ProjectDir() string {
	return s.manifest.ProjectDir
}

func (s *State) Naming() *model.Naming {
	return s.manifest.Naming
}

func (s *State) Components() *remote.ComponentsCache {
	return s.api.Components()
}

func (s *State) LocalManager() *local.Manager {
	return s.localManager
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
			logger.Warn("\t- ", path)
		}
	}
}

func (s *State) UntrackedPaths() []string {
	return s.paths.Untracked()
}

func (s *State) UntrackedDirs() (dirs []string) {
	for _, path := range s.paths.Untracked() {
		if !utils.IsDir(filepath.Join(s.manifest.ProjectDir, path)) {
			continue
		}
		dirs = append(dirs, path)
	}
	return dirs
}

func (s *State) RemoteErrors() *utils.Error {
	return s.remoteErrors
}

func (s *State) LocalErrors() *utils.Error {
	return s.localErrors
}

func (s *State) AddRemoteError(err error) {
	s.remoteErrors.Append(err)
}

func (s *State) AddLocalError(err error) {
	s.localErrors.Append(err)
}

func (s *State) All() []model.ObjectState {
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

	// Convert to slice, ignore deleted
	var slice []model.ObjectState
	for _, pair := range all {
		if pair.state.Manifest().State().IsDeleted() {
			continue
		}

		slice = append(slice, pair.state)
	}
	return slice
}

func (s *State) Branches() []*model.BranchState {
	var branches []*model.BranchState
	for _, b := range s.branches {
		if b.Manifest().State().IsDeleted() {
			continue
		}
		branches = append(branches, b)
	}
	sort.SliceStable(branches, func(i, j int) bool {
		return branches[i].SortKey(s.manifest.SortBy) < branches[j].SortKey(s.manifest.SortBy)
	})
	return branches
}

func (s *State) Configs() []*model.ConfigState {
	var configs []*model.ConfigState
	for _, c := range s.configs {
		if c.Manifest().State().IsDeleted() {
			continue
		}
		configs = append(configs, c)
	}
	sort.SliceStable(configs, func(i, j int) bool {
		return configs[i].SortKey(s.manifest.SortBy) < configs[j].SortKey(s.manifest.SortBy)
	})
	return configs
}

func (s *State) ConfigRows() []*model.ConfigRowState {
	var configRows []*model.ConfigRowState
	for _, r := range s.configRows {
		if r.Manifest().State().IsDeleted() {
			continue
		}
		configRows = append(configRows, r)
	}
	sort.SliceStable(configRows, func(i, j int) bool {
		return configRows[i].SortKey(s.manifest.SortBy) < configRows[j].SortKey(s.manifest.SortBy)
	})
	return configRows
}

func (s *State) GetBranch(key model.BranchKey, create bool) *model.BranchState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.branches[keyStr]; !ok {
		if !create {
			return nil
		}
		s.branches[keyStr] = &model.BranchState{}
	}
	return s.branches[keyStr]
}

func (s *State) GetConfig(key model.ConfigKey, create bool) *model.ConfigState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.configs[keyStr]; !ok {
		if !create {
			return nil
		}
		s.configs[keyStr] = &model.ConfigState{}
	}
	return s.configs[keyStr]
}

func (s *State) GetConfigRow(key model.ConfigRowKey, create bool) *model.ConfigRowState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	keyStr := key.String()
	if _, ok := s.configRows[keyStr]; !ok {
		if !create {
			return nil
		}
		s.configRows[keyStr] = &model.ConfigRowState{}
	}
	return s.configRows[keyStr]
}

func (s *State) SetBranchRemoteState(remote *model.Branch) *model.BranchState {
	state := s.GetBranch(remote.BranchKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.BranchManifest == nil {
		state.BranchManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*model.BranchManifest)
		s.localManager.UpdatePaths(state, false)
	}
	return state
}

func (s *State) SetBranchLocalState(local *model.Branch, m *model.BranchManifest) *model.BranchState {
	state := s.GetBranch(local.BranchKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Local = local
	state.BranchManifest = m
	for _, path := range m.GetRelatedPaths() {
		s.MarkPathTracked(path)
	}
	return state
}

func (s *State) SetConfigRemoteState(remote *model.Config) *model.ConfigState {
	component, err := s.Components().Get(*remote.ComponentKey())
	if err != nil {
		s.AddRemoteError(err)
		return nil
	}

	state := s.GetConfig(remote.ConfigKey, true)
	state.Component = component
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.ConfigManifest == nil {
		state.ConfigManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*model.ConfigManifest)
		s.localManager.UpdatePaths(state, false)
	}
	return state
}

func (s *State) SetConfigLocalState(local *model.Config, m *model.ConfigManifest) *model.ConfigState {
	component, err := s.Components().Get(*local.ComponentKey())
	if err != nil {
		s.AddLocalError(err)
		return nil
	}

	state := s.GetConfig(local.ConfigKey, true)
	state.Component = component
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Local = local
	state.ConfigManifest = m
	for _, path := range m.GetRelatedPaths() {
		s.MarkPathTracked(path)
	}

	return state
}

func (s *State) SetConfigRowRemoteState(remote *model.ConfigRow) *model.ConfigRowState {
	state := s.GetConfigRow(remote.ConfigRowKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Remote = remote
	if state.ConfigRowManifest == nil {
		state.ConfigRowManifest = s.manifest.CreateOrGetRecord(remote.Key()).(*model.ConfigRowManifest)
		s.localManager.UpdatePaths(state, false)
	}
	return state
}

func (s *State) SetConfigRowLocalState(local *model.ConfigRow, m *model.ConfigRowManifest) *model.ConfigRowState {
	state := s.GetConfigRow(local.ConfigRowKey, true)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.Local = local
	state.ConfigRowManifest = m
	for _, path := range m.GetRelatedPaths() {
		s.MarkPathTracked(path)
	}
	return state
}

func (s *State) validate() {
	for _, component := range s.Components().AllLoaded() {
		if err := validator.Validate(component); err != nil {
			s.AddLocalError(utils.PrefixError(fmt.Sprintf(`component \"%s\" is not valid`, component.Key()), err))
		}
	}
	for _, objectState := range s.All() {
		if objectState.HasRemoteState() {
			if err := validator.Validate(objectState.RemoteState()); err != nil {
				s.AddRemoteError(utils.PrefixError(fmt.Sprintf(`%s \"%s\" is not valid`, objectState.Kind().Name, objectState.Key()), err))
			}
		}

		if objectState.HasLocalState() {
			if err := validator.Validate(objectState.LocalState()); err != nil {
				s.AddLocalError(utils.PrefixError(fmt.Sprintf(`%s \"%s\" is not valid`, objectState.Kind().Name, objectState.Key()), err))
			}
		}
	}
}
