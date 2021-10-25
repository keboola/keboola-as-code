package state

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// State - Local and Remote state of the project.
type State struct {
	*Options
	*model.State
	mutex         *sync.Mutex
	localManager  *local.Manager
	remoteManager *remote.Manager
	localErrors   *utils.Error
	remoteErrors  *utils.Error
}

type Options struct {
	fs              filesystem.Fs
	manifest        *manifest.Manifest
	api             *remote.StorageApi
	context         context.Context
	logger          *zap.SugaredLogger
	LoadLocalState  bool
	LoadRemoteState bool
	SkipNotFoundErr bool // not found error will be ignored
}

func NewOptions(m *manifest.Manifest, api *remote.StorageApi, ctx context.Context, logger *zap.SugaredLogger) *Options {
	return &Options{
		fs:       m.Fs(),
		manifest: m,
		api:      api,
		context:  ctx,
		logger:   logger,
	}
}

// LoadState - remote and local.
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
		state.loadRemoteState()
	}

	if state.LoadLocalState {
		state.logger.Debugf("Loading local state.")
		state.loadLocalState()
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
	}

	// State model struct
	s.State = model.NewState(options.logger, options.fs, options.api.Components(), options.manifest.SortBy)

	// Mapper
	mapperInst := mapper.New(options.logger, options.fs, options.manifest.Naming, s.State)

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(options.logger, options.fs, options.manifest, s.State, mapperInst)

	// Local manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, options.api, s.State, mapperInst)

	return s
}

func (s *State) Fs() filesystem.Fs {
	return s.manifest.Fs()
}

func (s *State) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *State) Naming() *model.Naming {
	return s.manifest.Naming
}

func (s *State) LocalManager() *local.Manager {
	return s.localManager
}

func (s *State) RemoteManager() *remote.Manager {
	return s.remoteManager
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

func (s *State) validate() {
	for _, component := range s.Components().AllLoaded() {
		if err := validator.Validate(component); err != nil {
			s.AddLocalError(utils.PrefixError(fmt.Sprintf(`component \"%s\" is not valid`, component.Key()), err))
		}
	}
	for _, objectState := range s.All() {
		if objectState.HasRemoteState() {
			if err := validator.Validate(objectState.RemoteState()); err != nil {
				s.AddRemoteError(utils.PrefixError(fmt.Sprintf(`remote %s is not valid`, objectState.Desc()), err))
			}
		}

		if objectState.HasLocalState() {
			if err := validator.Validate(objectState.LocalState()); err != nil {
				s.AddLocalError(utils.PrefixError(fmt.Sprintf(`local %s "%s" is not valid`, objectState.Kind(), objectState.Path()), err))
			}
		}
	}
}
