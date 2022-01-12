package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type relationsMapper struct {
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *relationsMapper {
	return &relationsMapper{state: s, logger: s.Logger()}
}
