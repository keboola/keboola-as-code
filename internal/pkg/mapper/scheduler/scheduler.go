package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
)

type schedulerMapper struct {
	model.MapperContext
	api *scheduler.Api
}

func NewMapper(context model.MapperContext, api *scheduler.Api) *schedulerMapper {
	return &schedulerMapper{MapperContext: context, api: api}
}
