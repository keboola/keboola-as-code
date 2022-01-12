package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// mapper saves shared codes (config rows) to "codes" local dir.
type mapper struct {
	*helper.SharedCodeHelper
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *mapper {
	return &mapper{state: s, logger: s.Logger(), SharedCodeHelper: helper.New(s)}
}
