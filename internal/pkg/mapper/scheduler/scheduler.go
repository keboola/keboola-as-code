package scheduler

import (
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type dependencies interface {
	SchedulerApiClient() (client.Sender, error)
}

type schedulerMapper struct {
	dependencies
	state *state.State
}

func NewMapper(s *state.State, d dependencies) *schedulerMapper {
	return &schedulerMapper{state: s, dependencies: d}
}
