package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type schedulerMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *schedulerMapper {
	return &schedulerMapper{MapperContext: context}
}
