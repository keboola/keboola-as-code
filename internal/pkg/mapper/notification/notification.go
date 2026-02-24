package notification

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type mapper struct {
	logger log.Logger
	state  *state.State
}

func NewMapper(s *state.State) *mapper {
	return &mapper{logger: s.Logger(), state: s}
}
