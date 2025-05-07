package state

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

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
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	validator       validator.Validator
	fileLoader      filesystem.FileLoader
	logger          log.Logger
	tracer          telemetry.Tracer
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
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func New(ctx context.Context, container ObjectsContainer, d dependencies) (s *State, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.declarative.state.new")
	defer span.End(&err)

	// Get dependencies
	logger := d.Logger()
	m := container.Manifest()
	api := d.KeboolaProjectAPI()
	components := d.Components()

	// Create mapper
	mapperInst := mapper.New()

	// Create file loader
	fileLoader := mapperInst.NewFileLoader(container.ObjectsRoot())

	knownPaths, err := knownpaths.New(ctx, container.ObjectsRoot(), knownpaths.WithFilter(fileLoader.IsIgnored))
	if err != nil {
		return nil, errors.PrefixError(err, "error loading directory structure")
	}

	// Create state
	namingRegistry := m.NamingRegistry()
	namingTemplate := m.NamingTemplate()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	pathMatcher := naming.NewPathMatcher(namingTemplate)
	s = &State{
		Registry:        NewRegistry(knownPaths, namingRegistry, components, m.SortBy()),
		container:       container,
		validator:       validator.New(),
		fileLoader:      fileLoader,
		tracer:          d.Telemetry().Tracer(),
		logger:          logger,
		manifest:        m,
		mapper:          mapperInst,
		namingGenerator: namingGenerator,
		pathMatcher:     pathMatcher,
	}

	// Local manager for load,save,delete ... operations
	s.localManager = local.NewManager(s.logger, s.validator, container.ObjectsRoot(), s.fileLoader, m, s.namingGenerator, s.Registry, s.mapper)

	// Remote manager for API operations
	s.remoteManager = remote.NewManager(s.localManager, api, s.Registry, s.mapper)

	// Create mappers
	mappers, err := container.MappersFor(s)
	if err != nil {
		return nil, err
	}
	s.mapper.AddMapper(mappers...)

	return s, nil
}

// Load - remote and local.
func (s *State) Load(ctx context.Context, options LoadOptions) (ok bool, localErr error, remoteErr error) {
	localErrors := errors.NewMultiError()
	remoteErrors := errors.NewMultiError()

	// Remote
	if options.LoadRemoteState {
		s.logger.Debugf(ctx, "Loading project remote state.")
		if err := s.loadRemoteState(ctx, options.RemoteFilter); err != nil {
			remoteErrors.Append(err)
		}
	}

	// Local
	if options.LoadLocalState {
		s.logger.Debugf(ctx, "Loading local state.")
		if err := s.loadLocalState(ctx, options.LocalFilter, options.IgnoreNotFoundErr); err != nil {
			localErrors.Append(err)
		}
	}

	// Validate
	localValidateErr, remoteValidateErr := s.Validate(ctx)
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

func (s *State) Validate(ctx context.Context) (error, error) {
	return s.validateLocal(ctx), s.validateRemote(ctx)
}

func (s *State) validateLocal(ctx context.Context) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.declarative.state.validation.local")
	defer span.End(&err)

	errs := errors.NewMultiError()
	for _, objectState := range s.All() {
		if objectState.HasLocalState() {
			if err := s.ValidateValue(objectState.LocalState()); err != nil {
				errs.AppendWithPrefixf(err, `local %s "%s" is not valid`, objectState.Kind(), objectState.Path())
			}
		}
	}
	return errs.ErrorOrNil()
}

func (s *State) validateRemote(ctx context.Context) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.declarative.state.validation.remote")
	defer span.End(&err)

	errs := errors.NewMultiError()
	for _, objectState := range s.All() {
		if objectState.HasRemoteState() {
			if err := s.ValidateValue(objectState.RemoteState()); err != nil {
				errs.AppendWithPrefixf(err, `remote %s is not valid`, objectState.Desc())
			}
		}
	}
	return errs.ErrorOrNil()
}

func (s *State) ValidateValue(value any) error {
	return s.validator.ValidateCtx(s.Ctx(), value, "dive", "")
}

// loadLocalState from manifest and local files to unified internal state.
func (s *State) loadLocalState(ctx context.Context, _filter *model.ObjectsFilter, ignoreNotFoundErr bool) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.declarative.state.load.local")
	defer span.End(&err)

	// Create filter if not set
	var filter model.ObjectsFilter
	if _filter != nil {
		filter = *_filter
	} else {
		filter = model.NoFilter()
	}

	uow := s.localManager.NewUnitOfWork(ctx)
	if ignoreNotFoundErr {
		uow.SkipNotFoundErr()
	}
	uow.LoadAll(s.manifest, filter)
	return uow.Invoke()
}

// loadRemoteState from API to unified internal state.
func (s *State) loadRemoteState(ctx context.Context, _filter *model.ObjectsFilter) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.declarative.state.load.remote")
	defer span.End(&err)

	// Create filter if not set
	var filter model.ObjectsFilter
	if _filter != nil {
		filter = *_filter
	} else {
		filter = model.NoFilter()
	}

	uow := s.remoteManager.NewUnitOfWork(ctx, "")
	uow.LoadAll(filter)
	return uow.Invoke()
}
