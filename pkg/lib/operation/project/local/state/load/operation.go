package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
)

type InvalidLocalStateError struct {
	error
}

func (e *InvalidLocalStateError) Unwrap() error {
	return e.error
}

type Options struct {
	IgnoreNotFoundError bool
	IgnoreAllErrors     bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
	ProjectDir() (filesystem.Fs, error)
}

func Run(o Options, d dependencies) (*local.State, error) {
	logger := d.Logger()

	// Filesystem
	fs, err := d.ProjectDir()
	if err != nil {
		return nil, err
	}

	// Project manifest
	options := loadProjectManifest.Options{IgnoreErrors: o.IgnoreAllErrors}
	manifest, err := loadProjectManifest.Run(fs, options, d)
	if err != nil {
		return nil, err
	}

	// Create state
	s, err := local.NewState(d, fs, manifest, project.LocalMappers(d))
	if err != nil {
		return nil, err
	}

	// Ignore not found error, if enabled
	ctx := d.Ctx()
	ctx = context.WithValue(ctx, local.IgnoreNotFoundError, o.IgnoreNotFoundError)

	// Load state
	logger.Debugf("Loading project local state.")
	uow := s.NewUnitOfWork(ctx, manifest.Filter())
	uow.LoadAll()
	if err := uow.Invoke(); err != nil {
		if !o.IgnoreAllErrors {
			return nil, InvalidLocalStateError{errors.PrefixError("project local state is invalid", err)}
		}
	}

	logger.Debugf("Project local state has been successfully loaded.")
	return s, nil
}
