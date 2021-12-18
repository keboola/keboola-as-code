package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
)

type schedulerMapper struct {
	mapper.Context
	api *scheduler.Api
}

func NewMapper(context mapper.Context, api *scheduler.Api) *schedulerMapper {
	return &schedulerMapper{Context: context, api: api}
}
