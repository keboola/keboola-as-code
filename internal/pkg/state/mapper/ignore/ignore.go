package ignore

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type ignoreMapper struct {
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *ignoreMapper {
	return &ignoreMapper{state: s, logger: s.Logger()}
}
