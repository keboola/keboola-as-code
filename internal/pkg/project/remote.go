package project

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
)

type remoteDependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

func RemoteMappers(d remoteDependencies) remote.MappersFactory {
	return func(s *remote.State) (mapper.Mappers, error) {
		return mapper.Mappers{}, nil
	}
}
