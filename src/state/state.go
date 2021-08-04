package state

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"keboola-as-code/src/components"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"
)

// State - Local and Remote state of the project
type State struct {
	*Options
	mutex        *sync.Mutex
	remoteErrors *utils.Error
	localErrors  *utils.Error
	paths        *PathsState
	localManager *local.Manager
	objects      *orderedmap.OrderedMap
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

	// Log allowed branches
	state.logger.Debugf(`Allowed branches: %s`, state.manifest.Content.AllowedBranches)

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
		objects:      utils.NewOrderedMap(),
	}
	s.localManager = local.NewManager(options.logger, options.manifest, s.api.Components())
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

func (s *State) Components() *components.Provider {
	return s.api.Components()
}

func (s *State) LocalManager() *local.Manager {
	return s.localManager
}

func (s *State) TrackedPaths() []string {
	return s.paths.Tracked()
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

func (s *State) LogUntrackedPaths(logger *zap.SugaredLogger) {
	untracked := s.UntrackedPaths()
	if len(untracked) > 0 {
		logger.Warn("Unknown paths found:")
		for _, path := range untracked {
			logger.Warn("\t- ", path)
		}
	}
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
	s.objects.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value().(model.ObjectState).Manifest().SortKey(s.manifest.SortBy)
		bKey := b.Value().(model.ObjectState).Manifest().SortKey(s.manifest.SortBy)
		return aKey < bKey
	})

	var out []model.ObjectState
	for _, key := range s.objects.Keys() {
		// Get value
		v, _ := s.objects.Get(key)
		object := v.(model.ObjectState)

		// Skip deleted
		if object.Manifest().State().IsDeleted() {
			continue
		}

		out = append(out, object)
	}

	return out
}

func (s *State) Branches() (branches []*model.BranchState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.BranchState); ok {
			branches = append(branches, v)
		}
	}
	return branches
}

func (s *State) Configs() (configs []*model.ConfigState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigState); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

func (s *State) ConfigRows() (rows []*model.ConfigRowState) {
	for _, object := range s.All() {
		if v, ok := object.(*model.ConfigRowState); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

func (s *State) Get(key model.Key) model.ObjectState {
	object, err := s.getOrCreate(key)
	if err != nil {
		panic(err)
	}

	if object == nil {
		panic(fmt.Errorf(`object "%s" not found`, key.String()))
	}
	return object
}

func (s *State) SetRemoteState(remote model.Object) model.ObjectState {
	state, err := s.getOrCreate(remote.Key())
	if err != nil {
		s.AddRemoteError(err)
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.SetRemoteState(remote)
	if !state.HasManifest() {
		// Generate manifest record, if not present (no local state)
		state.SetManifest(s.manifest.CreateOrGetRecord(remote.Key()))
		s.localManager.UpdatePaths(state, false)
	}
	return state
}

func (s *State) SetLocalState(local model.Object, record model.Record) model.ObjectState {
	state, err := s.getOrCreate(local.Key())
	if err != nil {
		s.AddLocalError(err)
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	state.SetLocalState(local)
	state.SetManifest(record)
	for _, path := range record.GetRelatedPaths() {
		s.paths.MarkTracked(path)
	}
	return state
}

func (s *State) getOrCreate(key model.Key) (model.ObjectState, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if v, ok := s.objects.Get(key.String()); ok {
		// Get
		return v.(model.ObjectState), nil
	} else {
		// Create
		var object model.ObjectState
		switch k := key.(type) {
		case model.BranchKey:
			object = &model.BranchState{}
		case model.ConfigKey:
			if component, err := s.Components().Get(*k.ComponentKey()); err == nil {
				object = &model.ConfigState{Component: component}
			} else {
				return nil, err
			}

		case model.ConfigRowKey:
			object = &model.ConfigRowState{}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, key))
		}

		s.objects.Set(key.String(), object)
		return object, nil
	}
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
