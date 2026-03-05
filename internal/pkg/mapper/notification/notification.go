package notification

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type mapper struct {
	state *state.State
}

func NewMapper(s *state.State) *mapper {
	return &mapper{state: s}
}
