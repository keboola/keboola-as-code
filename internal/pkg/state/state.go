package state

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
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
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Registry = registry.Registry

func NewRegistry(logger log.Logger, fs filesystem.Fs, components *model.ComponentsMap, sortBy string) *Registry {
	return registry.New(logger, fs, components, sortBy)
}

// State - Local and Remote state of the project.
type State struct {
	*Registry
	ctx             context.Context
	logger          log.Logger
	fs              filesystem.Fs
	manifest        manifest.Manifest
	mapper          *mapper.Mapper
	namingGenerator *naming.Generator
	pathMatcher     *naming.PathMatcher
	localManager    *local.Manager
	remoteManager   *remote.Manager
}

type Options struct {
	LoadLocalState    bool
	LoadRemoteState   bool
	IgnoreNotFoundErr bool // not found error will be ignored
}

// ObjectsContainer is Project or Template.
type ObjectsContainer interface {
	Fs() filesystem.Fs
	Manifest() manifest.Manifest
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
}

// LoadState - remote and local.
func LoadState(container ObjectsContainer, options Options, d dependencies) (state *State, ok bool, localErr error, remoteErr error) {
	localErrors := utils.NewMultiError()
	remoteErrors := utils.NewMultiError()
	state, err := NewState(container, d)
	if err != nil {
		return nil, false, err, err
	}

	// Remote
	if options.LoadRemoteState {
		state.logger.Debugf("Loading project remote state.")
		remoteErrors.Append(state.loadRemoteState())
	}

	// Local
	if options.LoadLocalState {
		state.logger.Debugf("Loading local state.")
		localErrors.Append(state.loadLocalState(options))
	}

	// Validate
	localValidateErr, remoteValidateErr := state.Validate()
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

func NewState(container ObjectsContainer, d dependencies) (*State, error) {
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}
	schedulerApi, err := d.SchedulerApi()
	if err != nil {
		return nil, err
	}

	m := container.Manifest()
	namingGenerator := naming.NewGenerator(m.NamingTemplate(), m.NamingRegistry())
	pathMatcher := naming.NewPathMatcher(m.NamingTemplate())

	// Create state
	s := &State{
		ctx:             d.Ctx(),
		logger:          d.Logger(),
		fs:              container.Fs(),
		manifest:        m,
		namingGenerator: namingGenerator,
		pathMatcher:     pathMatcher,
	}
	s.Registry = NewRegistry(s.logger, s.fs, storageApi.Components(), m.SortBy())

	// Mapper
	mapperContext := mapper.Context{
		Logger:          d.Logger(),
		Fs:              container.Fs(),
		NamingGenerator: namingGenerator,
		NamingRegistry:  m.NamingRegistry(),
		State:           s.Registry,
	}

	s.mapper = mapper.New()

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(s.logger, s.fs, m, namingGenerator, s.Registry, s.mapper)

	// Local manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, storageApi, s.Registry, s.mapper)

	mappers := []interface{}{
		// Core files
		description.NewMapper(),
		// Storage
		defaultbucket.NewMapper(s.localManager, mapperContext),
		// Variables
		variables.NewMapper(mapperContext),
		sharedcode.NewVariablesMapper(mapperContext),
		// Special components
		schedulerMapper.NewMapper(mapperContext, schedulerApi),
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

	return s, nil
}

func (s *State) Fs() filesystem.Fs {
	return s.fs
}

func (s *State) Manifest() manifest.Manifest {
	return s.manifest
}

func (s *State) NamingGenerator() *naming.Generator {
	return s.namingGenerator
}

func (s *State) PathMatcher() *naming.PathMatcher {
	return s.pathMatcher
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

func (s *State) Validate() (error, error) {
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
func (s *State) loadLocalState(options Options) error {
	errors := utils.NewMultiError()

	uow := s.localManager.NewUnitOfWork(s.ctx)
	if options.IgnoreNotFoundErr {
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
	uow := s.remoteManager.NewUnitOfWork(s.ctx, "")
	uow.LoadAll()
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}
	return errors.ErrorOrNil()
}
