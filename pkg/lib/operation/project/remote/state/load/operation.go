package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type InvalidRemoteStateError struct {
	error
}

func (e *InvalidRemoteStateError) Unwrap() error {
	return e.error
}

type Options struct {
	Filter *model.ObjectsFilter
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

func Run(o Options, d dependencies) (*remote.State, error) {
	logger := d.Logger()

	// Process filter value
	filter := model.NoFilter()
	if o.Filter == nil {
		filter = *o.Filter
	}

	// Create state
	s, err := remote.NewState(d, state.NewIdSorter(), project.RemoteMappers(d))
	if err != nil {
		return nil, err
	}

	// Load state
	logger.Debugf("Loading project remote state.")
	uow := s.NewUnitOfWork(d.Ctx(), filter, "")
	uow.LoadAll()
	if err := uow.Invoke(); err != nil {
		return nil, InvalidRemoteStateError{utils.PrefixError("project remote state is invalid", err)}
	}

	logger.Debugf("Project remote state has been successfully loaded.")
	return s, nil
}
