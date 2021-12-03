package load

import (
	"context"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type InvalidRemoteStateError struct {
	error
}

type InvalidLocalStateError struct {
	error
}

func (e *InvalidRemoteStateError) Unwrap() error {
	return e.error
}

func (e *InvalidLocalStateError) Unwrap() error {
	return e.error
}

type Options struct {
	LoadLocalState          bool
	LoadRemoteState         bool
	IgnoreNotFoundErr       bool
	IgnoreInvalidLocalState bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
	ProjectManifest() (*manifest.Manifest, error)
}

func Run(o Options, d dependencies) (*state.State, error) {
	ctx := d.Ctx()
	logger := d.Logger()

	// Get manifest
	projectManifest, err := d.ProjectManifest()
	if err != nil {
		return nil, err
	}

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// Get Scheduler API
	schedulerApi, err := d.SchedulerApi()
	if err != nil {
		return nil, err
	}

	stateOptions := state.NewOptions(projectManifest, storageApi, schedulerApi, ctx, logger)
	stateOptions.LoadLocalState = o.LoadLocalState
	stateOptions.LoadRemoteState = o.LoadRemoteState
	stateOptions.IgnoreNotFoundErr = o.IgnoreNotFoundErr

	projectState, ok := state.LoadState(stateOptions)
	if ok {
		logger.Debugf("Project state has been successfully loaded.")
	} else {
		if projectState.RemoteErrors().Len() > 0 {
			return nil, InvalidRemoteStateError{utils.PrefixError("cannot load project remote state", projectState.RemoteErrors())}
		}
		if projectState.LocalErrors().Len() > 0 {
			if o.IgnoreInvalidLocalState {
				logger.Info(`Ignoring invalid local state.`)
			} else {
				return nil, InvalidLocalStateError{utils.PrefixError("project local state is invalid", projectState.LocalErrors())}
			}
		}
	}

	return projectState, nil
}
