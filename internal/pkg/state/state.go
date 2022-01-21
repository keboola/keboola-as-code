package state

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Registry = registry.Registry

func NewRegistry(paths *knownpaths.Paths, namingRegistry *naming.Registry, components *model.ComponentsMap, sortBy string) *Registry {
	return registry.New(paths, namingRegistry, components, sortBy)
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

type LoadOptions struct {
	LoadLocalState    bool
	LoadRemoteState   bool
	IgnoreNotFoundErr bool // not found error will be ignored
	LocalFilter       *model.ObjectsFilter
	RemoteFilter      *model.ObjectsFilter
}

// ObjectsContainer is Project or Template.
type ObjectsContainer interface {
	Fs() filesystem.Fs
	Manifest() manifest.Manifest
	MappersFor(state *State) mapper.Mappers
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
}

func New(container ObjectsContainer, d dependencies) (*State, error) {
	// Get dependencies
	logger := d.Logger()
	fs := container.Fs()
	m := container.Manifest()
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}
	knownPaths, err := knownpaths.New(container.Fs())
	if err != nil {
		return nil, utils.PrefixError(`error loading directory structure`, err)
	}

	// Create state
	namingRegistry := m.NamingRegistry()
	namingTemplate := m.NamingTemplate()
	s := &State{
		Registry:        NewRegistry(knownPaths, namingRegistry, storageApi.Components(), m.SortBy()),
		ctx:             d.Ctx(),
		fs:              fs,
		logger:          logger,
		manifest:        m,
		namingGenerator: naming.NewGenerator(namingTemplate, namingRegistry),
		pathMatcher:     naming.NewPathMatcher(namingTemplate),
	}

	s.mapper = mapper.New()

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(s.logger, fs, m, s.namingGenerator, s.Registry, s.mapper)

	// Local manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, storageApi, s.Registry, s.mapper)

	s.mapper.AddMapper(container.MappersFor(s)...)

	return s, nil
}

// Load - remote and local.
func (s *State) Load(options LoadOptions) (ok bool, localErr error, remoteErr error) {
	localErrors := utils.NewMultiError()
	remoteErrors := utils.NewMultiError()

	// Remote
	if options.LoadRemoteState {
		s.logger.Debugf("Loading project remote state.")
		remoteErrors.Append(s.loadRemoteState(options.RemoteFilter))
	}

	// Local
	if options.LoadLocalState {
		s.logger.Debugf("Loading local state.")
		localErrors.Append(s.loadLocalState(options.LocalFilter, options.IgnoreNotFoundErr))
	}

	// Validate
	localValidateErr, remoteValidateErr := s.Validate()
	if localValidateErr != nil {
		localErrors.Append(localValidateErr)
	}
	if remoteValidateErr != nil {
		remoteErrors.Append(remoteValidateErr)
	}

	// Process errors
	ok = localErrors.Len() == 0 && remoteErrors.Len() == 0
	return ok, localErrors.ErrorOrNil(), remoteErrors.ErrorOrNil()
}

func (s *State) Logger() log.Logger {
	return s.logger
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

// loadLocalState from manifest and local files to unified internal state.
func (s *State) loadLocalState(_filter *model.ObjectsFilter, ignoreNotFoundErr bool) error {
	// Create filter if not set
	var filter model.ObjectsFilter
	if _filter != nil {
		filter = *_filter
	} else {
		filter = model.NoFilter()
	}

	uow := s.localManager.NewUnitOfWork(s.ctx)
	if ignoreNotFoundErr {
		uow.SkipNotFoundErr()
	}
	uow.LoadAll(s.manifest, filter)
	return uow.Invoke()
}

// loadRemoteState from API to unified internal state.
func (s *State) loadRemoteState(_filter *model.ObjectsFilter) error {
	// Create filter if not set
	var filter model.ObjectsFilter
	if _filter != nil {
		filter = *_filter
	} else {
		filter = model.NoFilter()
	}

	uow := s.remoteManager.NewUnitOfWork(s.ctx, "")
	uow.LoadAll(filter)
	return uow.Invoke()
}
