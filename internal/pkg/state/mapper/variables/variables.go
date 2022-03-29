package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type variablesMapper struct {
	state *state.State
}

func NewMapper(s *state.State) *variablesMapper {
	return &variablesMapper{state: s}
}
