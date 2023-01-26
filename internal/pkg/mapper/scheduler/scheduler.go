package scheduler

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type dependencies interface {
	KeboolaAPIClient() *keboola.API
}

type schedulerMapper struct {
	dependencies
	state *state.State
}

func NewMapper(s *state.State, d dependencies) *schedulerMapper {
	return &schedulerMapper{state: s, dependencies: d}
}
