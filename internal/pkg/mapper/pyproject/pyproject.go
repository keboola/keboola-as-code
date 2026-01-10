package pyproject

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// pyprojectMapper generates pyproject.toml for Python transformations.
type pyprojectMapper struct {
	state  *state.State
	logger log.Logger
}

// NewMapper creates a new pyproject mapper.
func NewMapper(s *state.State) *pyprojectMapper {
	return &pyprojectMapper{
		state:  s,
		logger: s.Logger(),
	}
}
