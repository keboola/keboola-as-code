package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/description"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	schedulerMapper "github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// State - Local and Remote state of the project.
type State struct {
	*Options
	*model.State
	mutex         *sync.Mutex
	mapper        *mapper.Mapper
	localManager  *local.Manager
	remoteManager *remote.Manager
}

type Options struct {
	fs                filesystem.Fs
	manifest          *manifest.Manifest
	api               *remote.StorageApi
	schedulerApi      *scheduler.Api
	context           context.Context
	logger            log.Logger
	LoadLocalState    bool
	LoadRemoteState   bool
	IgnoreNotFoundErr bool // not found error will be ignored
}

func NewOptions(projectDir filesystem.Fs, m *manifest.Manifest, api *remote.StorageApi, schedulerApi *scheduler.Api, ctx context.Context, logger log.Logger) *Options {
	return &Options{
		fs:           projectDir,
		manifest:     m,
		api:          api,
		schedulerApi: schedulerApi,
		context:      ctx,
		logger:       logger,
	}
}

// LoadState - remote and local.
func LoadState(options *Options) (state *State, ok bool, localErr error, remoteErr error) {
	localErrors := utils.NewMultiError()
	remoteErrors := utils.NewMultiError()
	state = newState(options)

	// Log allowed branches
	state.logger.Debugf(`Allowed branches: %s`, state.manifest.AllowedBranches())

	// Remote
	if state.LoadRemoteState {
		state.logger.Debugf("Loading project remote state.")
		remoteErrors.Append(state.loadRemoteState())
	}

	// Local
	if state.LoadLocalState {
		state.logger.Debugf("Loading local state.")
		localErrors.Append(state.loadLocalState())
	}

	// Validate
	localValidateErr, remoteValidateErr := state.validate()
	if localValidateErr != nil {
		localErrors.Append(localValidateErr)
	}
	if remoteValidateErr != nil {
		remoteErrors.Append(remoteValidateErr)
	}

	// Process errors
	ok = localErrors.Len() == 0 && remoteErrors.Len() == 0
	return state, ok, localErrors.ErrorOrNil(), remoteErrors.ErrorOrNil()
}

func newState(options *Options) *State {
	s := &State{Options: options, mutex: &sync.Mutex{}}

	// State model struct
	s.State = model.NewState(options.logger, options.fs, options.api.Components(), options.manifest.SortBy())

	// Mapper
	mapperContext := model.MapperContext{
		Logger: options.logger,
		Fs:     options.fs,
		Naming: options.manifest.Naming(),
		State:  s.State,
	}

	s.mapper = mapper.New(mapperContext)

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(options.logger, options.fs, options.manifest, s.State, s.mapper)

	// Local manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, options.api, s.State, s.mapper)

	mappers := []interface{}{
		// Core files
		description.NewMapper(),
		// Storage
		defaultbucket.NewMapper(s.localManager, mapperContext),
		// Variables
		variables.NewMapper(mapperContext),
		sharedcode.NewVariablesMapper(mapperContext),
		// Special components
		schedulerMapper.NewMapper(mapperContext, options.schedulerApi),
		orchestrator.NewMapper(s.localManager, mapperContext),
		// Native codes
		transformation.NewMapper(mapperContext),
		sharedcode.NewCodesMapper(mapperContext),
		// Shared code links
		sharedcode.NewLinksMapper(s.localManager, mapperContext),
		// Relations between objects
		relations.NewMapper(mapperContext),
	}
	s.mapper.AddMapper(mappers...)

	return s
}

func (s *State) Fs() filesystem.Fs {
	return s.fs
}

func (s *State) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *State) Naming() *model.Naming {
	return s.manifest.Naming()
}

func (s *State) Mapper() *mapper.Mapper {
	return s.mapper
}

func (s *State) LocalManager() *local.Manager {
	return s.localManager
}

func (s *State) RemoteManager() *remote.Manager {
	return s.remoteManager
}

func (s *State) validate() (error, error) {
	localErrors := utils.NewMultiError()
	remoteErrors := utils.NewMultiError()

	for _, component := range s.Components().AllLoaded() {
		if err := validator.Validate(component); err != nil {
			localErrors.Append(utils.PrefixError(fmt.Sprintf(`component \"%s\" is not valid`, component.Key()), err))
		}
	}

	for _, objectState := range s.All() {
		if objectState.HasRemoteState() {
			if err := validator.Validate(objectState.RemoteState()); err != nil {
				remoteErrors.Append(utils.PrefixError(fmt.Sprintf(`remote %s is not valid`, objectState.Desc()), err))
			}
		}

		if objectState.HasLocalState() {
			if err := validator.Validate(objectState.LocalState()); err != nil {
				localErrors.Append(utils.PrefixError(fmt.Sprintf(`local %s "%s" is not valid`, objectState.Kind(), objectState.Path()), err))
			}
		}
	}

	return localErrors.ErrorOrNil(), remoteErrors.ErrorOrNil()
}

// loadLocalState - manifest -> local files -> unified model.
func (s *State) loadLocalState() error {
	errors := utils.NewMultiError()

	uow := s.localManager.NewUnitOfWork(s.context)
	if s.IgnoreNotFoundErr {
		uow.SkipNotFoundErr()
	}

	uow.LoadAll(s.manifest)
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

// loadRemoteState - API -> unified model.
func (s *State) loadRemoteState() error {
	errors := utils.NewMultiError()
	uow := s.remoteManager.NewUnitOfWork(s.context, "")
	uow.LoadAll()
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}
	return errors.ErrorOrNil()
}
