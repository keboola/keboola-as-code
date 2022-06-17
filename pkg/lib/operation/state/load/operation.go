package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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

type OptionsWithFilter struct {
	Options
	LocalFilter  *model.ObjectsFilter
	RemoteFilter *model.ObjectsFilter
}

func InitOptions(pull bool) Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         pull,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func DiffOptions() Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func PullOptions(force bool) Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: force,
	}
}

func PushOptions() Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         true,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func PersistOptions() Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       true,
		IgnoreInvalidLocalState: false,
	}
}

func LocalOperationOptions() Options {
	return Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

type dependencies interface {
	Logger() log.Logger
	StorageApiClient() (client.Sender, error)
}

func Run(container state.ObjectsContainer, o OptionsWithFilter, d dependencies) (*state.State, error) {
	logger := d.Logger()
	loadOptions := state.LoadOptions{
		LoadLocalState:    o.LoadLocalState,
		LoadRemoteState:   o.LoadRemoteState,
		IgnoreNotFoundErr: o.IgnoreNotFoundErr,
		LocalFilter:       o.LocalFilter,
		RemoteFilter:      o.RemoteFilter,
	}

	// Create state
	projectState, err := state.New(container, d)
	if err != nil {
		return nil, err
	}

	// Load objects
	ok, localErr, remoteErr := projectState.Load(loadOptions)
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
