package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/sharedcode/helper"
)

// mapper embeds variables config according "variables_id".
type mapper struct {
	state *state.State
	*helper.SharedCodeHelper
}

func NewMapper(s *state.State) *mapper {
	return &mapper{state: s, SharedCodeHelper: helper.New(s)}
}
