package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
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
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
}

func Run(o Options, d dependencies) (*state.State, error) {
	ctx := d.Ctx()
	logger := d.Logger()

	// Get project dir
	projectDir, err := d.ProjectDir()
	if err != nil {
		return nil, err
	}

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

	stateOptions := state.NewOptions(projectDir, projectManifest, storageApi, schedulerApi, ctx, logger)
	stateOptions.LoadLocalState = o.LoadLocalState
	stateOptions.LoadRemoteState = o.LoadRemoteState
	stateOptions.IgnoreNotFoundErr = o.IgnoreNotFoundErr

	projectState, ok, localErr, remoteErr := state.LoadState(stateOptions)
	if ok {
		logger.Debugf("Project state has been successfully loaded.")
	} else {
		if remoteErr != nil {
			return nil, InvalidRemoteStateError{utils.PrefixError("cannot load project remote state", remoteErr)}
		}
		if localErr != nil {
			if o.IgnoreInvalidLocalState {
				logger.Info(`Ignoring invalid local state.`)
			} else {
				return nil, InvalidLocalStateError{utils.PrefixError("project local state is invalid", localErr)}
			}
		}
	}

	return projectState, nil
}
