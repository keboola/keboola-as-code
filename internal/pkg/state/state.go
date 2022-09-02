package state

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
	"github.com/keboola/keboola-as-code/internal/pkg/state/remote"
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
	container       ObjectsContainer
	fileLoader      filesystem.FileLoader
	logger          log.Logger
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
	Ctx() context.Context
	ObjectsRoot() filesystem.Fs
	Manifest() manifest.Manifest
	MappersFor(state *State) (mapper.Mappers, error)
}

type dependencies interface {
	Logger() log.Logger
	Components() *model.ComponentsMap
	StorageApiClient() client.Sender
}

func New(container ObjectsContainer, d dependencies) (*State, error) {
	// Get dependencies
	logger := d.Logger()
	m := container.Manifest()
	storageApi := d.StorageApiClient()
	components := d.Components()

	// Create mapper
	mapperInst := mapper.New()

	// Create file loader
	fileLoader := mapperInst.NewFileLoader(container.ObjectsRoot())

	knownPaths, err := knownpaths.New(container.ObjectsRoot(), knownpaths.WithFilter(fileLoader.IsIgnored))
	if err != nil {
		return nil, utils.PrefixError(`error loading directory structure`, err)
	}

	// Create state
	namingRegistry := m.NamingRegistry()
	namingTemplate := m.NamingTemplate()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	pathMatcher := naming.NewPathMatcher(namingTemplate)
	s := &State{
		Registry:        NewRegistry(knownPaths, namingRegistry, components, m.SortBy()),
		container:       container,
		fileLoader:      fileLoader,
		logger:          logger,
		manifest:        m,
		mapper:          mapperInst,
		namingGenerator: namingGenerator,
		pathMatcher:     pathMatcher,
	}

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(s.logger, container.ObjectsRoot(), s.fileLoader, m, s.namingGenerator, s.Registry, s.mapper)

	// Remote manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, storageApi, s.Registry, s.mapper)

	// Create mappers
	mappers, err := container.MappersFor(s)
	if err != nil {
		return nil, err
	}
	s.mapper.AddMapper(mappers...)

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

func (s *State) Ctx() context.Context {
	return s.container.Ctx()
}

func (s *State) ObjectsRoot() filesystem.Fs {
	return s.container.ObjectsRoot()
}

func (s *State) FileLoader() filesystem.FileLoader {
	return s.fileLoader
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

	for _, objectState := range s.All() {
		if objectState.HasRemoteState() {
			if err := s.validateValue(objectState.RemoteState()); err != nil {
				remoteErrors.Append(utils.PrefixError(fmt.Sprintf(`remote %s is not valid`, objectState.Desc()), err))
			}
		}

		if objectState.HasLocalState() {
			if err := s.validateValue(objectState.LocalState()); err != nil {
				localErrors.Append(utils.PrefixError(fmt.Sprintf(`local %s "%s" is not valid`, objectState.Kind(), objectState.Path()), err))
			}
		}
	}

	return localErrors.ErrorOrNil(), remoteErrors.ErrorOrNil()
}

func (s *State) validateValue(value interface{}) error {
	return validator.ValidateCtx(s.Ctx(), value, "dive", "")
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

	uow := s.localManager.NewUnitOfWork(s.Ctx())
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

	uow := s.remoteManager.NewUnitOfWork(s.Ctx(), "")
	uow.LoadAll(filter)
	return uow.Invoke()
}
