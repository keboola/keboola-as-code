package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type dependencies interface {
	SchedulerApi() (*schedulerapi.Api, error)
}

type schedulerMapper struct {
	dependencies
	state *state.State
}

func NewMapper(s *state.State, d dependencies) *schedulerMapper {
	return &schedulerMapper{state: s, dependencies: d}
}
