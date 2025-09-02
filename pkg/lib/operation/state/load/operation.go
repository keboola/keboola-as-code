package load

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type InvalidRemoteStateError struct {
	error
}

type InvalidLocalStateError struct {
	error
}

func (e InvalidRemoteStateError) Error() string {
	return e.MainError().Error() + ": " + e.error.Error()
}

func (e InvalidRemoteStateError) Unwrap() error {
	return e.error
}

func (e InvalidRemoteStateError) MainError() error {
	return errors.New("cannot load remote state")
}

func (e InvalidRemoteStateError) WrappedErrors() []error {
	return []error{e.error}
}

func (e InvalidLocalStateError) Error() string {
	return e.MainError().Error() + ": " + e.error.Error()
}

func (e InvalidLocalStateError) Unwrap() error {
	return e.error
}

func (e InvalidLocalStateError) MainError() error {
	return errors.New("cannot load local state")
}

func (e InvalidLocalStateError) WrappedErrors() []error {
	return []error{e.error}
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
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, container state.ObjectsContainer, o OptionsWithFilter, d dependencies) (s *state.State, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.state.load")
	span.SetAttributes(attribute.Bool("remote.load", o.LoadRemoteState))
	span.SetAttributes(attribute.String("remote.filter", json.MustEncodeString(o.RemoteFilter, false)))
	span.SetAttributes(attribute.Bool("local.load", o.LoadLocalState))
	span.SetAttributes(attribute.String("local.filter", json.MustEncodeString(o.LocalFilter, false)))
	defer span.End(&err)

	logger := d.Logger()
	loadOptions := state.LoadOptions{
		LoadLocalState:    o.LoadLocalState,
		LoadRemoteState:   o.LoadRemoteState,
		IgnoreNotFoundErr: o.IgnoreNotFoundErr,
		LocalFilter:       o.LocalFilter,
		RemoteFilter:      o.RemoteFilter,
	}

	// Create state
	projectState, err := state.New(ctx, container, d)
	if err != nil {
		return nil, err
	}

	// Load objects
	ok, localErr, remoteErr := projectState.Load(ctx, loadOptions)
	if ok {
		logger.Debugf(ctx, "Project state has been successfully loaded.")
	} else {
		if remoteErr != nil {
			return nil, InvalidRemoteStateError{error: remoteErr}
		}
		if localErr != nil {
			if o.IgnoreInvalidLocalState {
				logger.Info(ctx, `Ignoring invalid local state.`)
			} else {
				return nil, InvalidLocalStateError{error: localErr}
			}
		}
	}

	return projectState, nil
}
